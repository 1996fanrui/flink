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
 * Replays each entry of a frozen {@link SpillFile} into the corresponding physical channel's {@code
 * recoveredBuffers} queue, and serves the task-thread checkpoint trigger via {@link
 * RecoveryCheckpointTrigger}.
 *
 * <p>Concurrency model: a single private {@code lock} guards three things together so the
 * checkpoint snapshot never observes a half-applied entry: (a) per-entry channel delivery via
 * {@code onRecoveredStateBuffer}; (b) advancing {@code currentSegmentIndex} / {@code
 * currentOffset}; (c) the checkpoint disk snapshot plus per-channel barrier insert. Buffer
 * allocation and disk read run outside the lock; only the deliver + advance pair is inside.
 * End-of-drain {@code finishRecoveredBufferDelivery} runs outside the lock — no more buffers are
 * being added at that point.
 */
@Internal
public final class SpillFileReader implements RecoveryCheckpointTrigger, Closeable {

    private final SpillFile spillFile;

    /**
     * Resolved channels (list + {@link InputChannelInfo}-keyed map). Reader construction happens
     * before channel conversion, so the physical channel set arrives later via the input future;
     * the derived map is computed once by {@code thenApply} and shared by drain and snapshot.
     */
    private final CompletableFuture<ResolvedChannels> resolvedChannelsFuture;

    /**
     * Drain holds this lock briefly per entry; the task thread holds it once per checkpoint
     * trigger. Lock order: {@code SpillFileReader.lock → channel-internal queue monitor}.
     */
    private final Object lock = new Object();

    @GuardedBy("lock")
    private int currentSegmentIndex = 0;

    @GuardedBy("lock")
    private long currentOffset = 0L;

    /**
     * @param channelsFuture completed with the post-conversion physical channel set; carries both
     *     the synchronization signal and the channels themselves.
     */
    public SpillFileReader(
            SpillFile spillFile, CompletableFuture<List<RecoverableInputChannel>> channelsFuture) {
        this.spillFile = checkNotNull(spillFile);
        this.resolvedChannelsFuture = checkNotNull(channelsFuture).thenApply(ResolvedChannels::new);
        // One ref-count grant for the reader's lifetime; matched by close().
        spillFile.acquire();
    }

    /** Cached pair of the physical channel list and its {@link InputChannelInfo}-keyed map. */
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
     * Drains every entry in the underlying spill file sequentially. Buffer allocation and disk read
     * happen outside the lock; only the channel deliver plus drain-progress advance is inside.
     */
    public void drain() throws IOException, InterruptedException {
        // The channels future is guaranteed complete before drain runs: channel conversion on the
        // mailbox finishes before the mailbox suspends, which is what releases this thread.
        ResolvedChannels channels = resolvedChannelsFuture.join();
        for (SpillFile.SpillFileSegment seg : spillFile.segments()) {
            SpillFile.Entry e;
            // Peek runs outside the lock: the entry queue is frozen (filter completed before
            // drain) and drain is its only mutator; the checkpoint snapshot is read-only on
            // entries.
            while ((e = seg.peekNextEntry()) != null) {
                RecoverableInputChannel ch = channels.channelByInfo.get(e.channelInfo);
                if (ch == null) {
                    throw new IllegalStateException(
                            "Drain: no physical channel found for " + e.channelInfo);
                }

                // Buffer allocation may park on BufferManager.bufferQueue; the channel's own
                // requestRecoveryBufferBlocking awaits upstream readiness internally and must run
                // outside the checkpoint-trigger lock so a concurrent snapshot can still proceed.
                Buffer buf;
                try {
                    buf = ch.requestRecoveryBufferBlocking();
                } catch (CompletionException | CancellationException releaseDuringAwait) {
                    return;
                }

                byte[] data = new byte[e.length];
                seg.readBytesAt(e.offset, e.length, data);
                buf.getMemorySegment().put(buf.getMemorySegmentOffset(), data, 0, e.length);
                buf.setSize(e.length);

                // Deliver + advance offset must be atomic so the checkpoint snapshot never sees a
                // half-applied entry.
                synchronized (lock) {
                    ch.onRecoveredStateBuffer(buf);
                    seg.pollNextEntry();
                    currentSegmentIndex = seg.segmentIndex;
                    currentOffset = e.offset + e.length;
                }
            }
        }
        // End-of-drain: finish flips allDelivered=true so the next consumer poll probes the
        // physical-channel upstream. finishRecoveredBufferDelivery awaits upstream readiness
        // internally; channels with no spill entries also reach this loop so they observe the
        // upstream-ready edge before being marked delivered.
        for (RecoverableInputChannel ch : channels.allChannels) {
            try {
                ch.finishRecoveredBufferDelivery();
            } catch (CompletionException | CancellationException releaseDuringAwait) {
                // Channel released mid-drain; the release path tears it down.
            }
        }
    }

    @Override
    public DiskSnapshot snapshotAndInsertBarriers(long checkpointId) throws IOException {
        // By the time the checkpoint barrier handler reaches this on the task thread,
        // requestPartitions has completed and the channels future is guaranteed done.
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

            // A channel needs a RecoveryCheckpointBarrier iff its recovery queue is still
            // in-recovery (allDelivered=false OR queue non-empty). Driving this off the global
            // drain cursor instead of per-channel state would miss channels whose sentinel is
            // still queued after the cursor has reached end-of-spill.
            for (RecoverableInputChannel ch : channels.allChannels) {
                if (ch.isInRecovery()) {
                    ch.onRecoveredStateBuffer(
                            EventSerializer.toBuffer(
                                    new RecoveryCheckpointBarrier(checkpointId), false));
                }
            }

            if (diskSliceEmpty) {
                // No data to persist; the empty singleton's close() is a no-op, so do NOT take a
                // grant — otherwise we leak one per checkpoint.
                return DiskSnapshot.empty();
            }

            // Paired with DiskSnapshot.close() (released on both success and abort paths).
            spillFile.acquire();
        }

        return new DiskSnapshot(
                diskSnap, new DiskSnapshot.StartPos(startSegmentIndex, startOffset), spillFile);
    }

    /**
     * Releases the drain's grant on the spill file. Segment deletion happens once all outstanding
     * grants ({@link DiskSnapshot}s included) are released. Source channels manage their own
     * BufferManager lifecycle through {@code releaseAllResources()} on the physical channel.
     */
    @Override
    public void close() throws IOException {
        spillFile.release();
    }

    /**
     * True when every entry in {@code snap} has already been drained. Compared as the lexicographic
     * two-tuple {@code (segmentIndex, offset)}; the offset cursor sits one byte past the last
     * delivered entry.
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
