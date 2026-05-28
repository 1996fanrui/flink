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

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A snapshot of the spill-file state at the moment a recovery checkpoint was triggered. Fed into
 * the channel-state writer when persisting unaligned-checkpoint input state.
 *
 * <p>Iteration yields one {@link Chunk} per entry that falls strictly after {@link StartPos};
 * entries already delivered by the drain are skipped. {@link StartPos} is captured inside {@code
 * SpillFileReader.lock} at the same instant the {@link RecoveryCheckpointBarrier} sentinels are
 * inserted into channel queues, so the in-memory and on-disk portions are disjoint and cover every
 * entry exactly once.
 */
@Internal
public final class DiskSnapshot implements CloseableIterator<DiskSnapshot.Chunk> {

    /** A single recovered-state entry read from the spill file, belonging to one input channel. */
    public static final class Chunk {
        public final InputChannelInfo channelInfo;
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
     * {@code SpillFileReader.lock} together with the per-channel barrier inserts, so the disk
     * portion and the in-memory portion of the snapshot share a single physical instant.
     */
    public static final class StartPos {
        public final int segmentIndex;
        public final long offset;

        public StartPos(int segmentIndex, long offset) {
            this.segmentIndex = segmentIndex;
            this.offset = offset;
        }
    }

    private static final DiskSnapshot EMPTY_INSTANCE = new DiskSnapshot();

    /**
     * Returns a singleton {@code DiskSnapshot} over an empty range: iteration is immediately
     * exhausted and {@link #close()} is a no-op. Used when no spill file exists or the drain has
     * already consumed every entry.
     */
    public static DiskSnapshot empty() {
        return EMPTY_INSTANCE;
    }

    private final SpillFile.Snapshot snapshot;
    private final StartPos startPos;
    private final List<SpillFile.Entry> entries;

    /**
     * Back-reference to the {@link SpillFile} that produced this snapshot, so {@link #close()} can
     * release the ref-count grant taken at construction time. {@code null} only for the {@link
     * #empty()} singleton.
     */
    @Nullable private final SpillFile spillFile;

    private int entryCursor;
    private boolean closed;

    private DiskSnapshot() {
        this.snapshot = null;
        this.startPos = null;
        this.entries = Collections.emptyList();
        this.spillFile = null;
        this.entryCursor = 0;
        this.closed = false;
    }

    /**
     * Caller must have already invoked {@code spillFile.acquire()} inside {@code
     * SpillFileReader.lock} so the ref-count grant is paired with this instance's {@link #close()}.
     */
    public DiskSnapshot(SpillFile.Snapshot snapshot, StartPos startPos, SpillFile spillFile) {
        this.snapshot = snapshot;
        this.startPos = startPos;
        this.entries = snapshot.getEntries();
        this.spillFile = checkNotNull(spillFile);
        this.entryCursor = 0;
        this.closed = false;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }
        skipPreDrained();
        return entryCursor < entries.size();
    }

    @Override
    public Chunk next() {
        if (closed) {
            throw new NoSuchElementException("DiskSnapshot is closed");
        }
        skipPreDrained();
        if (entryCursor >= entries.size()) {
            throw new NoSuchElementException("DiskSnapshot is exhausted");
        }
        SpillFile.Entry e = entries.get(entryCursor++);
        byte[] data = new byte[e.length];
        try {
            snapshot.getSegments().get(e.segmentIndex).readBytesAt(e.offset, e.length, data);
        } catch (IOException ioe) {
            throw new RuntimeException(
                    "Failed to read spill entry from segment " + e.segmentIndex, ioe);
        }
        return new Chunk(e.channelInfo, data, e.length);
    }

    /**
     * Idempotently releases this snapshot's ref-count grant on the underlying {@link SpillFile}.
     * Segment deletion is gated by {@code SpillFile.cleanedUp}, so the file is removed only after
     * every reader plus the drain has released its grant.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (spillFile != null) {
            spillFile.release();
        }
    }

    /**
     * Skips entries whose {@code (segmentIndex, offset)} is strictly before {@link #startPos};
     * those have already been delivered by the drain and live in the channel queues, where the
     * caller observes them via the per-channel pre-barrier walk.
     */
    private void skipPreDrained() {
        if (startPos == null) {
            return;
        }
        while (entryCursor < entries.size()) {
            SpillFile.Entry e = entries.get(entryCursor);
            boolean preDrained =
                    e.segmentIndex < startPos.segmentIndex
                            || (e.segmentIndex == startPos.segmentIndex
                                    && e.offset < startPos.offset);
            if (!preDrained) {
                break;
            }
            entryCursor++;
        }
    }
}
