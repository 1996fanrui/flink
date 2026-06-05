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

import org.apache.flink.annotation.Internal;

import java.io.InputStream;

/**
 * One per-channel segment produced by {@link FetchedChannelStateReader#nextSegment()}.
 *
 * <p>The segment body bytes are opaque to the reader; record framing is handled by the consumer's
 * deserializer. A consumer reads {@link #body()} to EOF (after {@link #length()} bytes), and the
 * drain consumer additionally calls {@link #commit()} under the drainer lock after each delivery so
 * that a later {@link FetchedChannelStateReader#snapshot()} resumes from the delivered boundary.
 *
 * <p>Ownership of {@link #body()} passes to the consumer: the reader no longer tracks how far it
 * has been read. The "previous body must be fully read" rule (no skip-ahead) is enforced at the
 * next {@link FetchedChannelStateReader#nextSegment()} call, not here.
 *
 * <p>A segment is valid only until the next {@code nextSegment()} call on the parent reader.
 */
@Internal
public interface SpillSegment {

    /** The channel whose data this segment contains. */
    InputChannelInfo channelInfo();

    /**
     * Returns an {@link InputStream} bounded to this segment's body. Reading returns {@code -1}
     * (EOF) after {@link #length()} bytes; it never reads into the next segment or the next file.
     *
     * <p>The stream is single-use, not thread-safe, and must be fully consumed before the next
     * {@link FetchedChannelStateReader#nextSegment()}.
     */
    InputStream body();

    /**
     * Number of body bytes this segment hands out before EOF. For the snapshot path this is the
     * not-yet-delivered remainder used as the length prefix when writing to the checkpoint stream.
     * Bounded by the spill file size limit, so it always fits in an {@code int}.
     */
    int length();

    /**
     * Advances the reader's committed position to match how many body bytes have been read from
     * {@link #body()} so far. Must be called under the drainer lock after each buffer delivery so
     * that a subsequent {@link FetchedChannelStateReader#snapshot()} sees the correct delivered
     * boundary. Only the drain (root) reader commits; the snapshot reader never does.
     */
    void commit();
}
