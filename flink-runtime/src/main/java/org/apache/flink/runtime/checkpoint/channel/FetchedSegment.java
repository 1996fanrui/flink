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

/**
 * Locator for one per-channel segment in the spill file list.
 *
 * <p>One instance is created for each channel switch during writing. The segment body bytes are not
 * held in memory; only the offset and length needed to read from disk on demand are stored.
 *
 * <p>Instances are built during writing via {@link FetchedChannelState#appendSegment} and become
 * read-only after the writer is closed.
 */
@Internal
public final class FetchedSegment {

    /** The channel whose data this segment contains. */
    public final InputChannelInfo channelInfo;

    /**
     * Index into the file path list held by {@link FetchedChannelState}. A segment never spans two
     * files, so this single index uniquely identifies the file.
     */
    public final int fileIndex;

    /**
     * Byte offset of the first segment body byte within the file at {@code fileIndex}. Does not
     * include the 8-byte segment header (gateIdx + channelIdx) written before the body.
     */
    public final long offset;

    /** Byte length of the segment body (excluding the segment header). */
    public final long length;

    public FetchedSegment(InputChannelInfo channelInfo, int fileIndex, long offset, long length) {
        this.channelInfo = channelInfo;
        this.fileIndex = fileIndex;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public String toString() {
        return "FetchedSegment{"
                + "channelInfo="
                + channelInfo
                + ", fileIndex="
                + fileIndex
                + ", offset="
                + offset
                + ", length="
                + length
                + '}';
    }
}
