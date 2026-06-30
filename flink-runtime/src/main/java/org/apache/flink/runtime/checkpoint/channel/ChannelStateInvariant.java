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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * TEMPORARY diagnostic instrumentation for the channel-state recovery data-corruption hunt.
 *
 * <p>Idea (do NOT do opaque checksums): the rescale test emits records whose value is an 8-byte
 * long {@code AB CD EA FC | VV VV VV VV} (high 4 bytes are a fixed header, low 4 bytes an
 * increasing value). At every stage of the channel-state pipeline we decode the raw bytes back
 * down to the record level and verify that the known header {@code AB CD EA FC} is still present at
 * the expected stride. The earliest stage whose bytes no longer satisfy this invariant is the
 * stage that broke them. This turns "guess which component has the bug" into "let the bytes tell us
 * where they first went bad".
 *
 * <p>Gated by {@code -Dflink.cs.debug} (default ON). The throw-site shift extractor runs regardless.
 * Remove this class and all call sites once the root cause is found.
 */
public final class ChannelStateInvariant {

    private static final Logger LOG = LoggerFactory.getLogger(ChannelStateInvariant.class);

    /** Verbose per-buffer / per-stage logging. Default ON; disable with -Dflink.cs.debug=false. */
    public static final boolean ON = !"false".equalsIgnoreCase(System.getProperty("flink.cs.debug"));

    /** Per-record logging (extremely chatty). OFF unless -Dflink.cs.debug.records=true. */
    public static final boolean RECORDS = "true".equalsIgnoreCase(System.getProperty("flink.cs.debug.records"));

    /** High 4 bytes of {@code UnalignedCheckpointTestBase.HEADER} = 0xABCDEAFC. */
    private static final byte[] HEADER = {(byte) 0xAB, (byte) 0xCD, (byte) 0xEA, (byte) 0xFC};

    private static final int MAX_DUMP = 256;

    private ChannelStateInvariant() {}

    // ---------------------------------------------------------------------------------------------
    // byte extraction helpers
    // ---------------------------------------------------------------------------------------------

    /** Copies the readable region of a NIO buffer without disturbing its position. */
    public static byte[] toBytes(ByteBuffer bb) {
        ByteBuffer dup = bb.duplicate();
        byte[] out = new byte[dup.remaining()];
        dup.get(out);
        return out;
    }

    public static byte[] slice(byte[] src, int from, int len) {
        int n = Math.min(len, src.length - from);
        byte[] out = new byte[Math.max(0, n)];
        System.arraycopy(src, from, out, 0, out.length);
        return out;
    }

    // ---------------------------------------------------------------------------------------------
    // header / shape analysis (the actual diagnostic)
    // ---------------------------------------------------------------------------------------------

    /** All offsets in [from,to) where the 4-byte header AB CD EA FC starts. */
    public static List<Integer> findHeaders(byte[] b, int from, int to) {
        List<Integer> hits = new ArrayList<>();
        int end = Math.min(to, b.length) - HEADER.length;
        for (int i = Math.max(0, from); i <= end; i++) {
            if (b[i] == HEADER[0]
                    && b[i + 1] == HEADER[1]
                    && b[i + 2] == HEADER[2]
                    && b[i + 3] == HEADER[3]) {
                hits.add(i);
            }
        }
        return hits;
    }

    /**
     * Returns a one-line "shape" summary: where the headers are and the stride between them. A
     * healthy framed stream has a constant stride (== framed record size). The index at which the
     * stride first changes is where the byte stream went out of alignment.
     */
    public static String shape(byte[] b, int from, int len) {
        int to = from + len;
        List<Integer> h = findHeaders(b, from, to);
        StringBuilder sb = new StringBuilder();
        sb.append("len=").append(len).append(" headers=").append(h.size());
        if (h.isEmpty()) {
            sb.append(" NO-HEADER (relativeToFrom)");
            return sb.toString();
        }
        sb.append(" firstHeaderAt=").append(h.get(0) - from);
        // strides between consecutive headers
        StringBuilder strides = new StringBuilder("[");
        int prev = -1;
        boolean irregular = false;
        int expected = -1;
        for (int i = 0; i < h.size(); i++) {
            if (prev >= 0) {
                int d = h.get(i) - prev;
                if (expected < 0) {
                    expected = d;
                } else if (d != expected) {
                    irregular = true;
                }
                strides.append(d);
                if (i < h.size() - 1) {
                    strides.append(',');
                }
            }
            prev = h.get(i);
        }
        strides.append(']');
        sb.append(" strides=").append(strides);
        if (irregular) {
            sb.append(" *** STRIDE-IRREGULAR (alignment broke here) ***");
        }
        return sb.toString();
    }

    /** Hex dump of up to MAX_DUMP bytes from {@code from}. */
    public static String hex(byte[] b, int from, int len) {
        int n = Math.min(Math.min(len, MAX_DUMP), Math.max(0, b.length - from));
        StringBuilder sb = new StringBuilder(n * 3);
        for (int i = 0; i < n; i++) {
            sb.append(String.format("%02X", b[from + i] & 0xff));
            if ((i & 0xF) == 0xF) {
                sb.append('\n');
            } else {
                sb.append(' ');
            }
        }
        if (len > MAX_DUMP) {
            sb.append("...(+").append(len - MAX_DUMP).append(" bytes)");
        }
        return sb.toString();
    }

    // ---------------------------------------------------------------------------------------------
    // call-site entry points
    // ---------------------------------------------------------------------------------------------

    /** Analyze a buffer of {@code [4B length][record]...} framed bytes at one pipeline stage. */
    public static void stage(String stageName, Object ctx, byte[] b, int from, int len) {
        if (!ON) {
            return;
        }
        LOG.info("[CS-INV] {} ch={} {}", stageName, ctx, shape(b, from, len));
    }

    public static void stage(String stageName, Object ctx, ByteBuffer bb) {
        if (!ON) {
            return;
        }
        byte[] b = toBytes(bb);
        stage(stageName, ctx, b, 0, b.length);
    }

    /** Per-record trace (very chatty). */
    public static void record(String stageName, Object ctx, byte[] recordBytes) {
        if (!RECORDS) {
            return;
        }
        LOG.info(
                "[CS-INV-REC] {} ch={} recLen={} headerOk={} bytes={}",
                stageName,
                ctx,
                recordBytes.length,
                !findHeaders(recordBytes, 0, recordBytes.length).isEmpty(),
                hex(recordBytes, 0, recordBytes.length));
    }

    /**
     * Always-on extractor for the corruption moment. {@code recordsOk} = how many records were read
     * cleanly from this buffer before the failure. Dumps the buffer's header shape + hex so the
     * exact byte where alignment broke (and the shift amount vs the nearest header) is visible.
     */
    public static void corruptionSite(
            Object ctx, int recordsOk, byte[] b, int from, int len, Throwable t) {
        LOG.error(
                "[CS-INV-CORRUPT] ch={} recordsOkInThisBuffer={} {} \nhex:\n{}",
                ctx,
                recordsOk,
                shape(b, from, len),
                hex(b, from, len),
                t);
    }

    /**
     * Stable identity of a backing file/state handle so two channel-state handles that share the
     * same physical checkpoint file are recognizable in the log. Uses {@link System#identityHashCode}
     * (object identity) plus {@code toString()} (path / in-memory id) — two handles over the same
     * file will print the same trailing path even if their wrapper objects differ.
     */
    public static String handleId(Object streamStateHandle) {
        if (streamStateHandle == null) {
            return "delegate=null";
        }
        return "delegate#"
                + Integer.toHexString(System.identityHashCode(streamStateHandle))
                + "{"
                + streamStateHandle
                + "}";
    }

    /** Free-form diagnostic note, logged at INFO like {@link #stage}. Gated by {@link #ON}. */
    public static void note(String stage, String msg) {
        if (!ON) {
            return;
        }
        LOG.info("[CS-INV] {} {}", stage, msg);
    }

    /**
     * Fail-fast: the input offsets map must only ever receive {@link InputChannelInfo} keys and the
     * output offsets map only {@link ResultSubpartitionInfo} keys. A violation means an input/output
     * channel-state offset cross-wire in the shared checkpoint-file offset namespace. Always-on (not
     * gated by {@link #ON}) so the cross-wire is caught even with verbose logging disabled.
     */
    public static void assertKeyKind(String stage, boolean expectInput, Object key) {
        boolean isInput = key instanceof InputChannelInfo;
        boolean isOutput = key instanceof ResultSubpartitionInfo;
        boolean ok = expectInput ? isInput : isOutput;
        if (!ok) {
            String msg =
                    "[CS-INV-ASSERT] "
                            + stage
                            + " expected "
                            + (expectInput ? "InputChannelInfo" : "ResultSubpartitionInfo")
                            + " key but got "
                            + (key == null ? "null" : key.getClass().getName())
                            + " key="
                            + key;
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }
    }

    /** Fail-fast invariant: byte accounting at a handoff must be consistent. */
    public static void assertEq(String stageName, Object ctx, String what, long expected, long actual) {
        if (expected != actual) {
            String msg =
                    "[CS-INV-ASSERT] "
                            + stageName
                            + " ch="
                            + ctx
                            + " "
                            + what
                            + " expected="
                            + expected
                            + " actual="
                            + actual;
            LOG.error(msg);
            throw new IllegalStateException(msg);
        }
    }
}
