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
import java.util.concurrent.CompletableFuture;
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

    /**
     * Resolved channels (list + InputChannelInfo-keyed map) wrapped in a future. The reader is
     * constructed on {@code channelIOExecutor} before {@code convertRecoveredInputChannels} has run
     * on the mailbox; the physical channel set arrives later via the input future, and the derived
     * {@code channelByInfo} map is computed once by the {@code thenApply} callback (which caches
     * the result). Drain and the task-thread Step 1 snapshot both {@link CompletableFuture#join}
     * this — by the time either runs, the upstream input future is guaranteed complete (mail #A
     * finishes before {@code suspend} and before task RUNNING).
     */
    private final CompletableFuture<ResolvedChannels> resolvedChannelsFuture;

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
     * Constructs the reader from a frozen spill file and a future that will be completed with the
     * physical channel set once {@code convertRecoveredInputChannels} runs on the mailbox. The
     * future is the second of the two communications between {@code channelIOExecutor} and the
     * mailbox; it carries both the synchronization signal and the channels themselves.
     */
    public SpillFileReader(
            SpillFile spillFile,
            CompletableFuture<List<RecoverableInputChannel>> channelsFuture,
            BufferRequester bufferRequester) {
        this.spillFile = checkNotNull(spillFile);
        this.resolvedChannelsFuture = checkNotNull(channelsFuture).thenApply(ResolvedChannels::new);
        this.bufferRequester = checkNotNull(bufferRequester);
        // Drain holds one ref-count grant for the lifetime of this reader; matched by close().
        spillFile.acquire();
    }

    /**
     * Cached pair of the physical channel list and the {@code InputChannelInfo}-keyed map derived
     * from it. Built once by the {@code thenApply} callback so drain and Step 1 snapshot share a
     * single map instance.
     */
    private static final class ResolvedChannels {
        final List<RecoverableInputChannel> allChannels;
        final Map<InputChannelInfo, RecoverableInputChannel> channelByInfo;

        ResolvedChannels(List<RecoverableInputChannel> all) {
            this.allChannels = all;
            Map<InputChannelInfo, RecoverableInputChannel> byInfo = new HashMap<>();
            for (RecoverableInputChannel ch : all) {
                byInfo.put(ch.getChannelInfo(), ch);
            }
            this.channelByInfo = byInfo;
        }
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
        // Block until convertRecoveredInputChannels has handed off the physical channel set.
        // join() is fine: by the time drain runs, the input future is guaranteed complete (mail
        // #A finishes on the mailbox before suspend runs, which is what unblocks the channelIO
        // thread after F1). Returns the cached ResolvedChannels — both drain and Step 1 share it.
        ResolvedChannels channels = resolvedChannelsFuture.join();
        for (SpillFile.SpillFileSegment seg : spillFile.segments()) {
            SpillFile.Entry e;
            // Peek runs outside the lock: filter completed before drain started, so the per-
            // segment entry queue is frozen. Drain is its only consumer; the task thread's Step 1
            // snapshot only reads entries, never mutates them. Putting peek inside the lock would
            // add no safety and increase Step 1 contention.
            while ((e = seg.peekNextEntry()) != null) {
                RecoverableInputChannel ch = channels.channelByInfo.get(e.channelInfo);
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

                // (C) Wait for the upstream connection to be published — outside the lock so a
                //     concurrent task-thread Step 1 can still take the lock and insert its
                //     RecoveryCheckpointBarrier while we wait. Channel-internal push paths are
                //     intentionally lock-free wrt upstreamReady (task-thread callers would
                //     deadlock against the PartitionNotFoundException retrigger, which is itself
                //     a mailbox-scheduled mail). Release-time exceptions surface as
                //     CompletionException / CancellationException and terminate drain gracefully.
                try {
                    ch.awaitUpstreamReady();
                } catch (CompletionException | CancellationException releaseDuringAwait) {
                    buf.recycleBuffer();
                    return;
                }

                // (D) Critical section — deliver + advance offset must be a single atomic action
                //     so the task-thread snapshot never observes a half-applied entry.
                synchronized (lock) {
                    ch.onRecoveredStateBuffer(buf);
                    seg.pollNextEntry();
                    currentSegmentIndex = seg.segmentIndex;
                    currentOffset = e.offset + e.length;
                }
            }
        }
        // (E) End-of-drain: signal producer completion to every channel. Must await upstream on
        //     each channel before finish — finish flips allDelivered=true, so the very next
        //     consumer poll on a now not-in-recovery channel hits the not-in-recovery branch and
        //     probes subpartitionView. Channels that never had any buffer in the spill file
        //     (skipped by the push loop above) are precisely the ones the push-time await would
        //     not have covered; this loop closes that gap. Release-time exceptions surface as
        //     CompletionException / CancellationException — skip that channel and continue
        //     finishing the rest (release path tears them down on its own).
        for (RecoverableInputChannel ch : channels.allChannels) {
            try {
                ch.awaitUpstreamReady();
            } catch (CompletionException | CancellationException releaseDuringAwait) {
                continue;
            }
            ch.finishRecoveredBufferDelivery();
        }
    }

    @Override
    public DiskSnapshot snapshotAndInsertBarriers(long checkpointId) throws IOException {
        // Same await as drain — by the time the cp barrier handler reaches this on the task
        // thread, requestPartitions has completed and the input future is guaranteed done.
        ResolvedChannels channels = resolvedChannelsFuture.join();

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
            for (RecoverableInputChannel ch : channels.allChannels) {
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
