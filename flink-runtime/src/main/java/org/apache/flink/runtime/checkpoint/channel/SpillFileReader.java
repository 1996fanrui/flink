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
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;

import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Drives the drain phase of the recovery-checkpoint protocol: replays each entry of a frozen {@link
 * SpillFile} into the corresponding physical channel's {@code recoveredBuffers} queue, and serves
 * the task-thread Step 1 trigger via {@link RecoveryCheckpointTrigger}.
 *
 * <p>Concurrency model — a single private {@code Object lock} guards three things together: (a)
 * per-entry channel delivery via {@code onRecoveredStateBuffer}; (b) advancing {@code
 * currentSegmentIndex} / {@code currentOffset}; (c) the Step 1 disk snapshot plus per-channel
 * barrier insert. Inside drain, buffer allocation and the disk read run OUTSIDE the lock — the lock
 * is only taken to deliver the buffer into the channel and update the drain progress fields in a
 * single atomic action. End-of-drain {@code finishRecoveredBufferDelivery} runs outside the lock;
 * at that point no more buffers are being added so the (queue, offset) atomicity does not apply.
 */
@Internal
public final class SpillFileReader implements RecoveryCheckpointTrigger, Closeable {

    private final SpillFile spillFile;
    private final List<RecoverableInputChannel> allChannels;
    private final Map<InputChannelInfo, RecoverableInputChannel> channelByInfo;
    private final BufferRequester bufferRequester;

    /**
     * Named lock (not the implicit {@code this} monitor) so it is grep-able and {@link
     * GuardedBy}-annotated. Drain holds it microsecond-scale per entry; task thread holds it once
     * per checkpoint trigger. Lock order: {@code SpillFileReader.lock → channel-internal queue
     * monitor}.
     */
    private final Object lock = new Object();

    @GuardedBy("lock")
    private int currentSegmentIndex = 0;

    @GuardedBy("lock")
    private long currentOffset = 0L;

    /**
     * Constructs the reader from a frozen spill file and the physical channel set of the task.
     * Derives the {@code InputChannelInfo}-keyed map internally via {@link
     * RecoverableInputChannel#getChannelInfo()}.
     */
    public SpillFileReader(
            SpillFile spillFile,
            List<RecoverableInputChannel> allChannels,
            BufferRequester bufferRequester) {
        this.spillFile = checkNotNull(spillFile);
        this.allChannels = checkNotNull(allChannels);
        this.bufferRequester = checkNotNull(bufferRequester);
        Map<InputChannelInfo, RecoverableInputChannel> byInfo = new HashMap<>();
        for (RecoverableInputChannel ch : allChannels) {
            byInfo.put(ch.getChannelInfo(), ch);
        }
        this.channelByInfo = byInfo;
        // Drain holds one ref-count grant for the lifetime of this reader; matched by close().
        spillFile.acquire();
    }

    /**
     * Drains every entry in the underlying spill file sequentially. Called by the {@code
     * channelIOExecutor} after conversion completes. Buffer allocation (A) and disk read (B) happen
     * outside the lock. Only the two-statement critical section (C) — channel deliver plus
     * drain-progress advance — is inside the lock. End-of-drain {@code
     * finishRecoveredBufferDelivery} (D) is outside the lock: no more buffers are being added at
     * that point, so the (queue, offset) atomicity that the lock protects does not apply.
     */
    public void drain() throws IOException, InterruptedException {
        for (SpillFile.SpillFileSegment seg : spillFile.segments()) {
            SpillFile.Entry e;
            // Peek runs outside the lock: filter completed before drain started, so the per-
            // segment entry queue is frozen. Drain is its only consumer; the task thread's Step 1
            // snapshot only reads entries, never mutates them. Putting peek inside the lock would
            // add no safety and increase Step 1 contention.
            while ((e = seg.peekNextEntry()) != null) {
                RecoverableInputChannel ch = channelByInfo.get(e.channelInfo);
                if (ch == null) {
                    throw new IllegalStateException(
                            "Drain: no physical channel found for " + e.channelInfo);
                }

                // (A) Buffer allocation outside the lock — parks on BufferManager.bufferQueue,
                //     which must not happen while holding the checkpoint-trigger lock.
                Buffer buf = bufferRequester.requestBufferBlocking(e.channelInfo);

                // (B) Disk read outside the lock — buf is local to this iteration and not yet
                //     visible to any other thread.
                byte[] data = new byte[e.length];
                seg.readBytesAt(e.offset, e.length, data);
                buf.getMemorySegment().put(buf.getMemorySegmentOffset(), data, 0, e.length);
                buf.setSize(e.length);

                // (C) Critical section — deliver + advance offset must be a single atomic action
                //     so the task-thread snapshot never observes a half-applied entry. Channel
                //     pushes block on the channel's per-instance upstreamReady future; release
                //     completes that future exceptionally, surfacing here as a
                //     CompletionException / CancellationException. Treat it as graceful drain
                //     termination: recycle the in-flight buffer and exit the loop. The release
                //     path itself drains recoveredQueue separately.
                try {
                    synchronized (lock) {
                        ch.onRecoveredStateBuffer(buf);
                        seg.pollNextEntry();
                        currentSegmentIndex = seg.segmentIndex;
                        currentOffset = e.offset + e.length;
                    }
                } catch (CompletionException | CancellationException releaseDuringPush) {
                    buf.recycleBuffer();
                    return;
                }
            }
        }
        // (D) End-of-drain: signal producer completion to every channel. The flag is published
        //     through the channel's internal monitor that finishRecoveredBufferDelivery already
        //     takes. Same release-time exception handling as the push loop above: a release in
        //     flight just terminates drain cleanly.
        try {
            for (RecoverableInputChannel ch : allChannels) {
                ch.finishRecoveredBufferDelivery();
            }
        } catch (CompletionException | CancellationException releaseDuringFinish) {
            // Already finished for the channels we got through; the rest will see isReleased
            // when they eventually unblock.
        }
    }

    @Override
    public DiskSnapshot snapshotAndInsertBarriers(long checkpointId) throws IOException {
        SpillFile.Snapshot diskSnap;
        int startSegmentIndex;
        long startOffset;
        boolean diskSliceEmpty;

        synchronized (lock) {
            diskSnap = spillFile.snapshot();
            startSegmentIndex = currentSegmentIndex;
            startOffset = currentOffset;
            diskSliceEmpty = recoveryAlreadyDone(diskSnap, startSegmentIndex, startOffset);

            // Per-channel barrier insert: a channel needs a RecoveryCheckpointBarrier iff its
            // recovery queue is still in-recovery (allDelivered=false OR queue non-empty).
            // Using the global drain cursor here would diverge from the checkpointStarted path
            // (which decides per channel) and produce "Missing RecoveryCheckpointBarrier" when
            // the cursor reaches end-of-spill before a channel has flipped allDelivered or while
            // a sentinel still sits in its queue.
            //
            // The barrier-insert path takes ch.isInRecovery() under the channel's own monitor
            // and onRecoveredStateBuffer under the same monitor; lock order stays
            // SpillFileReader.lock → channel-internal queue monitor, identical to the drain main
            // path, so no new lock-order edge is introduced.
            for (RecoverableInputChannel ch : allChannels) {
                if (ch.isInRecovery()) {
                    ch.onRecoveredStateBuffer(
                            EventSerializer.toBuffer(
                                    new RecoveryCheckpointBarrier(checkpointId), false));
                }
            }

            if (diskSliceEmpty) {
                // Empty disk slice: no channel-state data needs persisting and the empty
                // singleton's close() is a no-op, so we MUST NOT take a ref-count grant — doing
                // so would leak one grant per checkpoint.
                return DiskSnapshot.empty();
            }

            // Pair this acquire with DiskSnapshot.close() — the writer-completion callback
            // attached by the dispatcher (success path) and the abort path both close the
            // snapshot, releasing the grant exactly once.
            spillFile.acquire();
        }

        return new DiskSnapshot(
                diskSnap, new DiskSnapshot.StartPos(startSegmentIndex, startOffset), spillFile);
    }

    /**
     * Releases the source channels' exclusive buffer pools, then releases the drain's ref-count
     * grant on the spill file. Actual segment deletion happens inside {@link SpillFile#release()}
     * only once both this grant and every {@link DiskSnapshot} grant have been released.
     */
    @Override
    public void close() throws IOException {
        try {
            bufferRequester.releaseExclusiveBuffers();
        } finally {
            spillFile.release();
        }
    }

    /**
     * True when every entry in {@code snap} has already been drained. Compared as the lexicographic
     * two-tuple {@code (segmentIndex, offset)}; an entry with index < cursor is fully drained, one
     * with index == cursor is drained only if its offset is strictly below the cursor offset (the
     * offset cursor sits one byte past the last delivered entry).
     */
    private static boolean recoveryAlreadyDone(
            SpillFile.Snapshot snap, int curSegment, long curOffset) {
        for (SpillFile.Entry e : snap.getEntries()) {
            boolean drained =
                    e.segmentIndex < curSegment
                            || (e.segmentIndex == curSegment && e.offset < curOffset);
            if (!drained) {
                return false;
            }
        }
        return true;
    }
}
