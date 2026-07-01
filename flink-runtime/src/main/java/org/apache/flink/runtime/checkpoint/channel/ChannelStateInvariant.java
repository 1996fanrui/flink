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
 * accumulates buffer bytes per key across the five {@link Layer} points in a channel's lifecycle
 * and, once a channel's data for that layer is complete, validates the concatenated stream against
 * the deterministic test-data shape: every record is {@code [4-byte length][record bytes]}, and
 * each record's value carries a fixed 4-byte header {@code AB CD EA FC} at a constant stride.
 *
 * <p>A record can legitimately be split across the upstream/downstream boundary: while a checkpoint
 * is being taken, part of a record may already have been sent downstream while the rest is still
 * sitting in the upstream output buffer that gets snapshotted. So the concatenated stream of an
 * upstream output snapshot may start or end mid-record, and that is not corruption. Every other
 * validated stream (any input-channel state, and the entire recovery chain regardless of whether
 * the bytes originated upstream or downstream) represents data that either starts at a genuine
 * record boundary by construction, or is reassembled into one virtual channel that must be gap-free
 * once reassembled; a dangling partial record there is a real bug. {@link Mode} selects which of
 * these two rulesets {@link #shape} applies. Under both rulesets, an irregular stride between two
 * header occurrences that both fall inside the fully-framed middle of the stream is always real
 * corruption: a genuine dangling partial record only ever affects the first/last header, never the
 * stride between two interior ones.
 *
 * <p>This class never throws and never alters the data path; it only logs. All accumulation buffers
 * are per-key and must be explicitly flushed by the call site once that key's data for the current
 * layer is fully collected, otherwise they leak memory.
 *
 * <p>Public so that call sites outside this package (e.g. {@code LocalInputChannel}, {@code
 * RemoteInputChannel}) can accumulate and validate a channel's buffers via {@link #append}, {@link
 * #flush}, {@link #key}, and {@link #label}, or validate an already-collected buffer set directly
 * with {@link #shape(byte[], Mode)}.
 */
public final class ChannelStateInvariant {

    /**
     * Selects how strictly {@link #shape} judges a stream's first/last record.
     *
     * <p>{@code STRICT} is for anything that must be gap-free by construction: input-channel state
     * (at write, receive, or recovery time) and every step of the recovery chain, including the
     * portion of it that originated as upstream output before being reorganized into a virtual
     * channel. {@code LENIENT} is only for a genuinely in-flight upstream snapshot, where a
     * dangling partial record at the start or end reflects a record actually being
     * mid-transmission.
     */
    public enum Mode {
        STRICT,
        LENIENT
    }

    /**
     * The five points in a channel's data lifecycle where the same logical byte stream is validated
     * once each, in this order: in-flight data is collected off the wire ({@code SNAPSHOT}), handed
     * to the checkpoint writer ({@code CHECKPOINT_WRITE}), read back on recovery ({@code
     * RECOVER_READ}), rewritten by the recovery filter ({@code RECOVER_REWRITE}), and migrated into
     * the physical channel that will actually serve it ({@code CHANNEL_RECEIVE}). The first layer
     * that reports a violation brackets where the data first went bad: between it and the previous
     * layer in this sequence.
     */
    public enum Layer {
        SNAPSHOT,
        CHECKPOINT_WRITE,
        RECOVER_READ,
        RECOVER_REWRITE,
        CHANNEL_RECEIVE
    }

    /**
     * Whether the validated stream is input-channel state or result-subpartition (output) state.
     * Both share the same accumulator/key structure but are different physical entities and must
     * never be aggregated together.
     */
    public enum Direction {
        INPUT,
        OUTPUT
    }

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

    /** Per-key accumulators, keyed independently for each {@link Layer}. */
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
     * Flushes and validates the accumulated bytes for {@code key} under {@link Mode#STRICT},
     * logging the result under {@code [CS-INV]}/{@code [CS-INV-ASSERT]}, then discards the
     * accumulator.
     *
     * <p>Every current caller of this overload ({@code CHECKPOINT_WRITE} input side, {@code
     * CHANNEL_RECEIVE}, {@code RECOVER_READ}, {@code RECOVER_REWRITE}) is a downstream or
     * recovery-chain layer, which must never contain a dangling partial record; see the class-level
     * javadoc.
     *
     * @param taskAndChannel human-readable "task=.. ch=.." label shared across layers
     */
    public static void flush(String key, String taskAndChannel, Layer layer) {
        flush(key, taskAndChannel, layer, Mode.STRICT);
    }

    /**
     * Flushes and validates the accumulated bytes for {@code key} under the given {@link Mode},
     * logging the result under {@code [CS-INV]}, then discards the accumulator. A violation is
     * logged at assertion level ({@code [CS-INV-ASSERT]}) unless it is only a dangling partial
     * record tolerated by {@code mode}, in which case it is logged at observational level ({@code
     * [CS-INV-TOLERATED]}).
     *
     * @param taskAndChannel human-readable "task=.. ch=.." label shared across layers
     */
    public static void flush(String key, String taskAndChannel, Layer layer, Mode mode) {
        if (!ENABLED) {
            return;
        }
        Accumulator acc = ACCUMULATORS.remove(key);
        if (acc == null) {
            return;
        }
        Shape shape = shape(acc.toByteArray(), mode);
        LOG.info(
                "[CS-INV] {} layer={} mode={} bytes={} {}",
                taskAndChannel,
                layer,
                mode,
                acc.size(),
                shape.summary());
        logViolationIfAny(shape, "layer=" + layer, taskAndChannel);
        if (LOG_RECORDS) {
            for (String recordLog : shape.recordLogs) {
                LOG.info("[CS-INV-REC] {} layer={} {}", taskAndChannel, layer, recordLog);
            }
        }
    }

    /**
     * Logs a {@link Shape} violation, if any, at the level matching its severity: real corruption
     * (a mid-stream stride irregularity, or any violation under {@link Mode#STRICT}) at assertion
     * level; a dangling partial record tolerated only under {@link Mode#LENIENT} at observational
     * level. Emits nothing when {@code shape} is valid.
     */
    private static void logViolationIfAny(Shape shape, String context, String taskAndChannel) {
        if (shape.valid) {
            return;
        }
        if (shape.toleratedEdgeOnly) {
            LOG.info(
                    "[CS-INV-TOLERATED] {} {} dangling partial record at stream edge (upstream-only,"
                            + " tolerated): {}",
                    taskAndChannel,
                    context,
                    shape.summary());
        } else {
            LOG.warn(
                    "[CS-INV-ASSERT] {} {} INVALID complete-channel-data: {}",
                    taskAndChannel,
                    context,
                    shape.summary());
        }
    }

    /**
     * Builds an opaque accumulator key identifying one channel instance's data for one layer and
     * direction during one lifecycle (one checkpoint write, one recovery pass, or one physical
     * channel construction).
     *
     * <p>{@code identity} must already encode every dimension the call site can obtain: at minimum
     * jobVertexID + subtaskIndex, plus a lifecycle discriminator (checkpoint id, recovery-pass
     * identity, or similar) so that keys from two different lifecycles never collide. {@code
     * channel} must encode gateIdx/channelIdx (or the output-side equivalent). See {@link Layer}
     * for what "layer" means here, and {@link Direction} for input vs. output.
     */
    public static String key(String identity, String channel, Layer layer, Direction direction) {
        return identity + "|" + channel + "|" + layer + "|" + direction;
    }

    /** Builds the shared "task=.. ch=.." label used in log lines across all five layers. */
    public static String label(String task, String channel) {
        return "task=" + task + " ch=" + channel;
    }

    /**
     * Validates a checkpoint snapshot: the set of buffers a channel collected for one barrier
     * before handing them to the checkpoint writer.
     *
     * <p>This snapshot is genuinely in-flight upstream output, so it is validated under {@link
     * Mode#LENIENT}: a record may legitimately be half-sent downstream and half still in this
     * snapshot, so a dangling partial record at the start or end of the concatenated snapshot is
     * tolerated and only logged observationally, not as an assertion.
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
            Shape shape = shape(concatenated, Mode.LENIENT);
            LOG.info(
                    "[CS-INV-SNAP] {} layer={} cp={} numBuffers={} bytes={} {}",
                    taskAndChannel,
                    Layer.SNAPSHOT,
                    barrierId,
                    retained.size(),
                    totalBytes,
                    shape.summary());
            logViolationIfAny(
                    shape, "layer=" + Layer.SNAPSHOT + " cp=" + barrierId, taskAndChannel);
        } finally {
            for (Buffer buffer : retained) {
                buffer.recycleBuffer();
            }
        }
    }

    /**
     * Scans {@code bytes} for occurrences of the fixed record-value header, computes the stride
     * between consecutive occurrences, and walks the buffer as a sequence of length-prefixed
     * records under {@link Mode#STRICT} (no leading/trailing dangling record tolerated).
     *
     * <p>Equivalent to {@code shape(bytes, Mode.STRICT)}.
     */
    public static Shape shape(byte[] bytes) {
        return shape(bytes, Mode.STRICT);
    }

    /**
     * Scans {@code bytes} for occurrences of the fixed record-value header, computes the stride
     * between consecutive occurrences, and walks the buffer as a sequence of length-prefixed
     * records.
     *
     * <p>Under {@link Mode#STRICT}, the walk must start at byte 0 and every record, including the
     * first and last, must be fully framed. Under {@link Mode#LENIENT}, a dangling partial record
     * is tolerated at the start (leading bytes before the first record boundary are skipped) and at
     * the end (a truncated final record is not flagged); only a framing violation found strictly
     * between two already-confirmed record boundaries is treated as a real violation.
     *
     * <p>Regardless of mode, a stride irregularity between two header occurrences that are both
     * interior to the parsed record sequence (i.e. not the leading/trailing dangling remainder) is
     * always a real violation: it cannot be explained by a merely-incomplete edge record.
     */
    public static Shape shape(byte[] bytes, Mode mode) {
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
        shape.mode = mode;
        shape.headerCount = headerOffsets.size();
        shape.firstHeaderAt = headerOffsets.isEmpty() ? -1 : headerOffsets.get(0);
        shape.strides = strides;
        shape.strideIrregular = strideIrregular;

        walkFraming(bytes, shape, mode);
        boolean framingOk = shape.firstCorruptRecordAt < 0;
        boolean noHeaderAtAll = shape.headerCount == 0;
        shape.valid = !strideIrregular && !noHeaderAtAll && framingOk;
        // A violation is a tolerated edge effect, not real corruption, only when: mode allows it,
        // the reliable mid-stream signal (stride irregularity) did not fire, and the only reason
        // shape is invalid is a dangling remainder at the very start or end of the stream (no
        // parseable record at all, or the walk stopped exactly at a tolerated trailing remainder).
        shape.toleratedEdgeOnly =
                !shape.valid
                        && mode == Mode.LENIENT
                        && !strideIrregular
                        && (noHeaderAtAll || shape.tailTolerated);
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
     * and the payload's first byte is a legal StreamElementSerializer tag.
     *
     * <p>Under {@link Mode#STRICT} the walk starts at {@code pos=0} and any framing violation,
     * including at the very first or very last record, is reported as {@code firstCorruptRecordAt}.
     * Under {@link Mode#LENIENT} the walk first skips forward, byte by byte, past any dangling
     * partial record at the start (bytes before the first offset at which a well-formed record
     * actually parses); reaching the end of {@code bytes} without being able to fully frame one
     * more record (whether because fewer than 4 bytes remain for the length prefix, the decoded
     * length is non-positive, or the decoded payload length runs past the end of {@code bytes}) is
     * treated as a tolerated trailing partial record and flagged via {@code shape.tailTolerated}
     * rather than {@code firstCorruptRecordAt}. A framing violation found while there is still
     * enough trailing data for more complete records is reported as real corruption the same as in
     * {@code STRICT} mode: that cannot be explained by a merely-incomplete edge record.
     */
    private static void walkFraming(byte[] bytes, Shape shape, Mode mode) {
        shape.firstCorruptRecordAt = -1;
        int pos = mode == Mode.LENIENT ? skipToFirstParseableRecord(bytes) : 0;
        int recordIndex = 0;
        while (pos < bytes.length) {
            if (pos + 4 > bytes.length) {
                // Fewer than 4 bytes left for even a length prefix.
                shape.tailTolerated = mode == Mode.LENIENT;
                break;
            }
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
                if (mode == Mode.LENIENT && length <= 0) {
                    // A non-positive length this close to the end of a lenient stream is
                    // indistinguishable from a truncated tail record whose length-prefix bytes
                    // were only partially written; tolerate it rather than flag a false
                    // corruption for a legitimately dangling tail.
                    shape.tailTolerated = true;
                } else if (mode == Mode.LENIENT
                        && length > 0
                        && payloadStart + length > bytes.length) {
                    // The declared payload runs past the end of bytes: a legitimately truncated
                    // tail record (only part of its payload had been produced/sent yet).
                    shape.tailTolerated = true;
                } else {
                    shape.firstCorruptRecordAt = pos;
                    shape.corruptRecordHexBefore = hexAround(bytes, pos, -16, 0);
                    shape.corruptRecordHexAfter = hexAround(bytes, pos, 0, 16);
                }
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

    /**
     * Finds the first offset at which a well-formed {@code [4B length][payload]} record actually
     * parses (length in range and a legal tag), by scanning forward byte by byte from 0. Bytes
     * before that offset are the dangling remainder of a record whose earlier half was already sent
     * downstream before this snapshot was taken; used only in {@link Mode#LENIENT}.
     *
     * <p>Returns 0 if a record parses immediately at offset 0, or if no offset in {@code bytes}
     * parses (the caller's subsequent walk then finds no complete records at all, which is reported
     * the same as an all-dangling snapshot rather than corruption).
     */
    private static int skipToFirstParseableRecord(byte[] bytes) {
        for (int pos = 0; pos + 4 <= bytes.length; pos++) {
            int length =
                    ((bytes[pos] & 0xFF) << 24)
                            | ((bytes[pos + 1] & 0xFF) << 16)
                            | ((bytes[pos + 2] & 0xFF) << 8)
                            | (bytes[pos + 3] & 0xFF);
            int payloadStart = pos + 4;
            boolean lengthValid = length > 0 && payloadStart + length <= bytes.length;
            if (!lengthValid) {
                continue;
            }
            int tag = bytes[payloadStart] & 0xFF;
            if (tag >= MIN_TAG && tag <= MAX_TAG) {
                return pos;
            }
        }
        return 0;
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
        Mode mode;
        int headerCount;
        int firstHeaderAt;
        List<Integer> strides = new ArrayList<>();
        boolean strideIrregular;
        int recordCount;
        int firstCorruptRecordAt = -1;
        String corruptRecordHexBefore = "";
        String corruptRecordHexAfter = "";
        boolean tailTolerated;
        boolean toleratedEdgeOnly;
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
            if (tailTolerated) {
                sb.append(" tail-tolerated");
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
