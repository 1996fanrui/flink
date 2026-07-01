/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Temporary diagnostic instrumentation for tracking channel-state data corruption ("Corrupt stream,
 * found tag: -NN").
 *
 * <p>The unit of corruption is an input channel, not a single buffer: a channel's buffers must be
 * concatenated in order to form one contiguous, fully-parseable record stream. This class
 * accumulates buffer bytes per (task, channel) key across three pipeline layers (checkpoint write,
 * recovery read, filter rewrite) and, once a channel's data for that layer is complete, validates
 * the concatenated stream against the deterministic test-data shape: every record is {@code [4-byte
 * length][record bytes]}, and each record's value carries a fixed 4-byte header {@code AB CD EA FC}
 * at a constant stride.
 *
 * <p>This class never throws and never alters the data path; it only logs. All accumulation buffers
 * are per-key and must be explicitly flushed by the call site once that key's data for the current
 * layer is fully collected, otherwise they leak memory.
 *
 * <p>Public so that call sites outside this package (e.g. {@code LocalInputChannel}, {@code
 * RemoteInputChannel}) can accumulate and validate a channel's buffers via {@link #append}, {@link
 * #flush}, {@link #key}, and {@link #label}, or validate an already-collected buffer set directly
 * with {@link #shape(byte[])}.
 */
public final class ChannelStateInvariant {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelStateInvariant.class);

    /** Enables/disables all invariant checking. On by default to catch corruption as it occurs. */
    private static final boolean ENABLED = !"false".equals(System.getProperty("flink.cs.debug"));

    /** Enables per-record logging (verbose); shape-only logging is used otherwise. */
    private static final boolean LOG_RECORDS =
            "true".equals(System.getProperty("flink.cs.debug.records"));

    private static final byte[] HEADER = {(byte) 0xAB, (byte) 0xCD, (byte) 0xEA, (byte) 0xFC};

    /** Minimum valid StreamElementSerializer tag (TAG_REC_WITH_TIMESTAMP). */
    private static final int MIN_TAG = 0;

    /** Maximum valid StreamElementSerializer tag (TAG_INTERNAL_WATERMARK). */
    private static final int MAX_TAG = 6;

    /** Per-(task, channel) accumulators, keyed independently for each of the three layers. */
    private static final Map<String, Accumulator> ACCUMULATORS = new ConcurrentHashMap<>();

    private ChannelStateInvariant() {}

    public static boolean isEnabled() {
        return ENABLED;
    }

    /**
     * Appends the readable bytes of {@code buffer} to the accumulator for {@code key}, without
     * consuming/moving the buffer's reader index.
     *
     * @param key opaque identifier combining task + channel + layer, see {@link #key}
     */
    public static void append(String key, ByteBuffer buffer) {
        if (!ENABLED) {
            return;
        }
        ByteBuffer readOnly = buffer.duplicate();
        byte[] bytes = new byte[readOnly.remaining()];
        readOnly.get(bytes);
        ACCUMULATORS.computeIfAbsent(key, k -> new Accumulator()).append(bytes);
    }

    /**
     * Flushes and validates the accumulated bytes for {@code key}, logging the result under {@code
     * [CS-INV]}/{@code [CS-INV-ASSERT]}, then discards the accumulator.
     *
     * @param taskAndChannel human-readable "task=.. ch=.." label shared across layers
     * @param layer one of "WRITE" (checkpoint write), "RECOVER" (recovery read), "REWRITE" (filter
     *     rewrite)
     */
    public static void flush(String key, String taskAndChannel, String layer) {
        if (!ENABLED) {
            return;
        }
        Accumulator acc = ACCUMULATORS.remove(key);
        if (acc == null) {
            return;
        }
        Shape shape = shape(acc.toByteArray());
        LOG.info(
                "[CS-INV] {} layer={} bytes={} {}",
                taskAndChannel,
                layer,
                acc.size(),
                shape.summary());
        if (!shape.valid) {
            LOG.warn(
                    "[CS-INV-ASSERT] {} layer={} INVALID complete-channel-data: {}",
                    taskAndChannel,
                    layer,
                    shape.summary());
        }
        if (LOG_RECORDS) {
            for (String recordLog : shape.recordLogs) {
                LOG.info("[CS-INV-REC] {} layer={} {}", taskAndChannel, layer, recordLog);
            }
        }
    }

    /** Builds an opaque accumulator key from task and channel identifiers plus the layer name. */
    public static String key(String task, String channel, String layer) {
        return task + "|" + channel + "|" + layer;
    }

    /** Builds the shared "task=.. ch=.." label used in log lines across all three layers. */
    public static String label(String task, String channel) {
        return "task=" + task + " ch=" + channel;
    }

    /**
     * Validates a checkpoint snapshot: the set of buffers a channel collected for one barrier
     * before handing them to the checkpoint writer.
     *
     * <p>Takes its own retained reference to each buffer and releases it before returning, so the
     * caller's own buffer references and their recycling are unaffected. Must be called outside of
     * any lock the caller holds while collecting the snapshot, since it copies bytes and logs.
     */
    public static void validateSnapshot(
            String taskAndChannel, long barrierId, List<Buffer> buffers) {
        if (!ENABLED) {
            return;
        }
        int totalBytes = 0;
        List<Buffer> retained = new ArrayList<>(buffers.size());
        for (Buffer buffer : buffers) {
            retained.add(buffer.retainBuffer());
        }
        try {
            for (Buffer buffer : retained) {
                totalBytes += buffer.readableBytes();
            }
            byte[] concatenated = new byte[totalBytes];
            int offset = 0;
            for (Buffer buffer : retained) {
                ByteBuffer readOnly = buffer.getNioBufferReadable();
                int readable = readOnly.remaining();
                readOnly.get(concatenated, offset, readable);
                offset += readable;
            }
            Shape shape = shape(concatenated);
            LOG.info(
                    "[CS-INV-SNAP] {} cp={} numBuffers={} bytes={} {}",
                    taskAndChannel,
                    barrierId,
                    retained.size(),
                    totalBytes,
                    shape.summary());
            if (!shape.valid) {
                LOG.warn(
                        "[CS-INV-SNAP-ASSERT] {} cp={} INVALID snapshot: {}",
                        taskAndChannel,
                        barrierId,
                        shape.summary());
            }
        } finally {
            for (Buffer buffer : retained) {
                buffer.recycleBuffer();
            }
        }
    }

    /**
     * Scans {@code bytes} for occurrences of the fixed record-value header, computes the stride
     * between consecutive occurrences, and attempts to walk the buffer as a sequence of
     * length-prefixed records, stopping at the first framing violation.
     */
    public static Shape shape(byte[] bytes) {
        List<Integer> headerOffsets = findHeaderOffsets(bytes);
        List<Integer> strides = new ArrayList<>();
        for (int i = 1; i < headerOffsets.size(); i++) {
            strides.add(headerOffsets.get(i) - headerOffsets.get(i - 1));
        }
        boolean strideIrregular = false;
        if (!strides.isEmpty()) {
            int first = strides.get(0);
            for (int s : strides) {
                if (s != first) {
                    strideIrregular = true;
                    break;
                }
            }
        }

        Shape shape = new Shape();
        shape.headerCount = headerOffsets.size();
        shape.firstHeaderAt = headerOffsets.isEmpty() ? -1 : headerOffsets.get(0);
        shape.strides = strides;
        shape.strideIrregular = strideIrregular;

        walkFraming(bytes, shape);
        shape.valid = !strideIrregular && shape.headerCount > 0 && shape.firstCorruptRecordAt < 0;
        return shape;
    }

    private static List<Integer> findHeaderOffsets(byte[] bytes) {
        List<Integer> offsets = new ArrayList<>();
        for (int i = 0; i + HEADER.length <= bytes.length; i++) {
            if (matchesHeader(bytes, i)) {
                offsets.add(i);
            }
        }
        return offsets;
    }

    private static boolean matchesHeader(byte[] bytes, int offset) {
        for (int j = 0; j < HEADER.length; j++) {
            if (bytes[offset + j] != HEADER[j]) {
                return false;
            }
        }
        return true;
    }

    /**
     * Walks {@code bytes} as [4B length][payload] records, validating that the length is in range
     * and the payload's first byte is a legal StreamElementSerializer tag. Stops and records the
     * offset of the first violation; does not attempt to recover/resync afterwards since a single
     * corrupt frame poisons all subsequent offsets.
     */
    private static void walkFraming(byte[] bytes, Shape shape) {
        int pos = 0;
        int recordIndex = 0;
        shape.firstCorruptRecordAt = -1;
        while (pos + 4 <= bytes.length) {
            int length =
                    ((bytes[pos] & 0xFF) << 24)
                            | ((bytes[pos + 1] & 0xFF) << 16)
                            | ((bytes[pos + 2] & 0xFF) << 8)
                            | (bytes[pos + 3] & 0xFF);
            int payloadStart = pos + 4;
            boolean lengthValid = length > 0 && payloadStart + length <= bytes.length;
            int tag = lengthValid ? (bytes[payloadStart] & 0xFF) : -1;
            boolean tagValid = lengthValid && tag >= MIN_TAG && tag <= MAX_TAG;

            if (!lengthValid || !tagValid) {
                shape.firstCorruptRecordAt = pos;
                shape.corruptRecordHexBefore = hexAround(bytes, pos, -16, 0);
                shape.corruptRecordHexAfter = hexAround(bytes, pos, 0, 16);
                break;
            }

            if (LOG_RECORDS) {
                shape.recordLogs.add(
                        "record#" + recordIndex + " at=" + pos + " len=" + length + " tag=" + tag);
            }
            pos = payloadStart + length;
            recordIndex++;
        }
        shape.recordCount = recordIndex;
    }

    private static String hexAround(byte[] bytes, int center, int from, int to) {
        int start = Math.max(0, center + from);
        int end = Math.min(bytes.length, center + to);
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++) {
            sb.append(String.format("%02X ", bytes[i]));
        }
        return sb.toString().trim();
    }

    /** Result of validating one channel's fully-concatenated byte stream. */
    public static final class Shape {
        int headerCount;
        int firstHeaderAt;
        List<Integer> strides = new ArrayList<>();
        boolean strideIrregular;
        int recordCount;
        int firstCorruptRecordAt = -1;
        String corruptRecordHexBefore = "";
        String corruptRecordHexAfter = "";
        boolean valid;
        final List<String> recordLogs = new ArrayList<>();

        public boolean isValid() {
            return valid;
        }

        public String summary() {
            StringBuilder sb = new StringBuilder();
            if (headerCount == 0) {
                sb.append("NO-HEADER");
            } else {
                sb.append("headers=")
                        .append(headerCount)
                        .append(" firstHeaderAt=")
                        .append(firstHeaderAt)
                        .append(" strides=")
                        .append(strides);
                if (strideIrregular) {
                    sb.append(" *** STRIDE-IRREGULAR ***");
                }
            }
            sb.append(" parsedRecords=").append(recordCount);
            if (firstCorruptRecordAt >= 0) {
                sb.append(" *** CORRUPT-RECORD-AT=")
                        .append(firstCorruptRecordAt)
                        .append(" before=[")
                        .append(corruptRecordHexBefore)
                        .append("] after=[")
                        .append(corruptRecordHexAfter)
                        .append("] ***");
            }
            return sb.toString();
        }
    }

    /** Growable byte accumulator; avoids repeated array copies via a chunk list. */
    private static final class Accumulator {
        private final List<byte[]> chunks = new ArrayList<>();
        private int size;

        synchronized void append(byte[] chunk) {
            chunks.add(chunk);
            size += chunk.length;
        }

        synchronized int size() {
            return size;
        }

        synchronized byte[] toByteArray() {
            byte[] result = new byte[size];
            int pos = 0;
            for (byte[] chunk : chunks) {
                System.arraycopy(chunk, 0, result, pos, chunk.length);
                pos += chunk.length;
            }
            return result;
        }
    }
}
