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
 * A single per-channel segment visited during sequential iteration over spill files.
 *
 * <p>The segment body bytes are opaque to the reader; record framing is handled by the consumer's
 * deserializer. Callers consume {@link #body()} up to EOF (after {@link #length()} bytes), then
 * call {@link #commitConsumed()} under the drainer lock before advancing to the next cursor.
 *
 * <p>A cursor instance is valid only until the next {@code next()} call on the parent iterator.
 */
@Internal
public interface FetchedSegmentCursor {

    /** The channel whose data this segment contains. */
    InputChannelInfo channelInfo();

    /**
     * Returns an {@link InputStream} bounded to this segment's {@code [offset, offset + length)}
     * bytes. Reading returns {@code -1} (EOF) after {@link #length()} bytes have been read; it
     * never reads into the next segment or the next file.
     *
     * <p>The stream must be fully consumed before the iterator advances. The stream is single-use
     * and is not thread-safe.
     */
    InputStream body();

    /**
     * Segment body length in bytes, as recorded in the in-memory segment locator table. Used by the
     * snapshot path as the length prefix when writing to the checkpoint stream. Equals the number
     * of bytes available in {@link #body()} before EOF.
     */
    long length();

    /**
     * Commits bytes already consumed from {@link #body()} to the reader's drain cursor so that a
     * subsequent {@link FetchedChannelStateReader#snapshot()} sees the correct "already delivered"
     * boundary. Must be called under the drainer lock after each buffer delivery.
     *
     * <p>This separates the out-of-lock disk read from the in-lock cursor advance, preserving the
     * same atomicity semantics as the previous {@code advance()} call.
     */
    void commitConsumed();
}
