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
import org.apache.flink.util.CloseableIterator;

import java.util.NoSuchElementException;

/**
 * A snapshot of the spill-file state at the moment a recovery checkpoint was triggered. Returned
 * by {@link RecoveryCheckpointTrigger#snapshotAndInsertBarriers} at Step 1 and fed into
 * {@link ChannelStateWriter#addInputDataFromSpill} at Step 3.
 *
 * <p>Iterating over a {@code DiskSnapshot} yields {@link Chunk} entries — one per spill-file
 * entry that falls within the snapshot window. The iteration starting point is captured as
 * {@link StartPos} inside the lock in {@code RecoveryCheckpointTrigger}, ensuring that entries
 * already delivered to channel queues before the checkpoint barrier are excluded from the disk
 * portion.
 *
 * <p>The real iteration logic (seeking to {@link StartPos} and reading entries) is introduced in
 * Phase 4. This skeleton provides the type declarations and the {@link #empty()} factory used by
 * the feature-off code path.
 */
@Internal
public final class DiskSnapshot implements CloseableIterator<DiskSnapshot.Chunk> {

    /**
     * A single recovered-state entry read from the spill file, belonging to one input channel.
     */
    public static final class Chunk {
        /** The channel this entry belongs to. */
        public final InputChannelInfo channelInfo;
        /** Raw bytes of the recovered buffer. */
        public final byte[] data;
        /** Number of valid bytes in {@link #data}. */
        public final int length;

        Chunk(InputChannelInfo channelInfo, byte[] data, int length) {
            this.channelInfo = channelInfo;
            this.data = data;
            this.length = length;
        }
    }

    /**
     * The position within the spill file at which this snapshot begins. Captured atomically inside
     * {@code SpillFileReader.lock} at the same instant that the {@link RecoveryCheckpointBarrier}
     * sentinels are inserted into the channel queues. This guarantees that the disk and memory
     * portions of the snapshot are disjoint and together cover all entries exactly once.
     *
     * <p>Phase 4 fills in the real seek logic; this skeleton declares the type so that the
     * interface contract can be established without churn.
     */
    public static final class StartPos {
        /** Index of the spill-file segment that is current at snapshot time. */
        public final int segmentIndex;
        /** Byte offset within that segment at snapshot time. */
        public final long offset;

        public StartPos(int segmentIndex, long offset) {
            this.segmentIndex = segmentIndex;
            this.offset = offset;
        }
    }

    private static final DiskSnapshot EMPTY_INSTANCE = new DiskSnapshot();

    /**
     * Returns a {@code DiskSnapshot} over an empty range: {@link #hasNext()} is always
     * {@code false}, {@link #next()} always throws {@link NoSuchElementException}, and
     * {@link #close()} is a no-op. Used by the feature-off code path where no spill file exists.
     *
     * @return the shared empty instance
     */
    public static DiskSnapshot empty() {
        return EMPTY_INSTANCE;
    }

    /**
     * Default constructor. At this skeleton phase the instance is always in the empty state;
     * Phase 4 will introduce the constructor that accepts a {@link StartPos} and the spill-file
     * handle.
     */
    DiskSnapshot() {}

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Chunk next() {
        throw new NoSuchElementException("DiskSnapshot is empty");
    }

    @Override
    public void close() {}
}
