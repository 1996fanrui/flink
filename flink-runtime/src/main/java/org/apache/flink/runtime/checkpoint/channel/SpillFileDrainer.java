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
import org.apache.flink.util.CloseableIterator;

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
 * Drains a {@link SpillFile} into the per-channel recovered-buffer queues and serves the
 * task-thread checkpoint trigger via {@link RecoveryCheckpointTrigger}.
 *
 * <p>Composition: holds a single root {@link SpillFileReader} obtained from {@link
 * SpillFile#reader()}. {@link #drain()} iterates that reader; {@link #snapshotAndInsertBarriers}
 * derives a sub-reader from it via {@link SpillFileReader#snapshot()} and hands it (wrapped as a
 * {@link CloseableIterator}) to the checkpoint write path.
 *
 * <p>Concurrency model: a single private {@code lock} guards three things together so the
 * checkpoint snapshot never observes a half-applied entry: (a) per-entry channel delivery via
 * {@code onRecoveredStateBuffer}; (b) advancing the root reader cursor; (c) the checkpoint snapshot
 * derivation plus per-channel barrier insert. Buffer allocation and disk read run outside the lock;
 * only the deliver + advance pair is inside. End-of-drain {@code finishRecoveredBufferDelivery}
 * runs outside the lock — no more buffers are being added at that point.
 */
@Internal
public final class SpillFileDrainer implements RecoveryCheckpointTrigger, Closeable {

    private final SpillFileReader rootReader;

    /**
     * Resolved channels (list + {@link InputChannelInfo}-keyed map). Drainer construction happens
     * before channel conversion, so the physical channel set arrives later via the input future;
     * the derived map is computed once by {@code thenApply} and shared by drain and snapshot.
     */
    private final CompletableFuture<ResolvedChannels> resolvedChannelsFuture;

    /**
     * Drain holds this lock briefly per entry; the task thread holds it once per checkpoint
     * trigger. Lock order: {@code drainer.lock → channel-internal queue monitor}.
     */
    private final Object lock = new Object();

    /**
     * Set true once the drain loop has delivered its last entry and stopped advancing the root
     * reader; guarded by {@code lock} so it is published atomically with the final cursor advance.
     * A checkpoint that fires after drain has finished observes this under the same lock and
     * returns an empty slice without touching the (about-to-be / already) closed root reader —
     * there is no recovery data left to snapshot once every channel has been marked delivered.
     */
    private boolean drainFinished;

    /**
     * @param channelsFuture completed with the post-conversion physical channel set; carries both
     *     the synchronization signal and the channels themselves.
     */
    public SpillFileDrainer(
            SpillFile spillFile, CompletableFuture<List<RecoverableInputChannel>> channelsFuture) {
        this.rootReader = spillFile.reader();
        this.resolvedChannelsFuture = checkNotNull(channelsFuture).thenApply(ResolvedChannels::new);
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
     * happen outside the lock; only the channel deliver plus root-reader advance is inside.
     */
    public void drain() throws IOException, InterruptedException {
        // The channels future is guaranteed complete before drain runs: channel conversion on the
        // mailbox finishes before the mailbox suspends, which is what releases this thread.
        ResolvedChannels channels = resolvedChannelsFuture.join();
        SpillFileReader.Chunk chunk;
        while ((chunk = rootReader.peek()) != null) {
            RecoverableInputChannel ch = channels.channelByInfo.get(chunk.channelInfo);
            if (ch == null) {
                throw new IllegalStateException(
                        "Drain: no physical channel found for " + chunk.channelInfo);
            }

            // (A) Outside the lock: physical channel allocates a buffer from its own pool;
            //     requestRecoveryBufferBlocking internally awaits upstream readiness and may park.
            Buffer buf;
            try {
                buf = ch.requestRecoveryBufferBlocking();
            } catch (CompletionException | CancellationException releaseDuringAwait) {
                return;
            }

            // (B) Outside the lock: copy the chunk's reusable bytes into the channel buffer.
            buf.getMemorySegment().put(buf.getMemorySegmentOffset(), chunk.data, 0, chunk.length);
            buf.setSize(chunk.length);

            // (C) Critical section — two in-memory actions, strongly coupled. The advance() call
            //     is the only place the root reader's cursor moves; pairing with delivery makes
            //     it impossible for snapshotAndInsertBarriers to observe a half-applied entry.
            synchronized (lock) {
                ch.onRecoveredStateBuffer(buf);
                rootReader.advance();
            }
        }

        // (D) Mark drain finished under the lock, paired with the final advance() above. After this
        // point the root reader is no longer advanced and is about to be closed, so a checkpoint
        // entering snapshotAndInsertBarriers must observe drainFinished and return an empty slice
        // rather than derive a sub-reader from the closing root reader.
        synchronized (lock) {
            drainFinished = true;
        }

        // (E) End-of-drain: finish flips allDelivered=true so the next consumer poll probes the
        // physical-channel upstream. finishRecoveredBufferDelivery awaits upstream readiness
        // internally; channels with no spill entries also reach this loop so they observe the
        // upstream-ready edge before being marked delivered.
        for (RecoverableInputChannel ch : channels.allChannels) {
            ch.finishRecoveredBufferDelivery();
        }
    }

    @Override
    public CloseableIterator<SpillFileReader.Chunk> snapshotAndInsertBarriers(long checkpointId)
            throws IOException {
        // By the time the checkpoint barrier handler reaches this on the task thread,
        // requestPartitions has completed and the channels future is guaranteed done.
        ResolvedChannels channels = resolvedChannelsFuture.join();

        SpillFileReader sub;
        synchronized (lock) {
            // A channel needs a RecoveryCheckpointBarrier iff its recovery queue is still
            // in-recovery (allDelivered=false OR queue non-empty). This is decided per channel,
            // symmetrically with the per-channel isInRecovery() check in checkpointStarted, and
            // must run regardless of drainFinished: the global drainFinished flips before the
            // per-channel finish() loop, so a channel may still be in-recovery (and have its
            // collectPreRecoveryBarrier called) even after drain has stopped touching the root
            // reader. Inserting only when drainFinished is false would leave such a channel
            // without the barrier it is about to be asked to collect.
            for (RecoverableInputChannel ch : channels.allChannels) {
                if (ch.isInRecovery()) {
                    ch.onRecoveredStateBuffer(
                            EventSerializer.toBuffer(
                                    new RecoveryCheckpointBarrier(checkpointId), false));
                }
            }

            // Drain has finished: the root reader is already (or about to be) closed, so there is
            // no on-disk slice left to snapshot. Return an empty slice before touching the root
            // reader, which close() may have already invalidated. Barrier insertion above is
            // independent of this and has already run.
            if (drainFinished) {
                return CloseableIterator.empty();
            }

            sub = rootReader.snapshot();
        }

        // Empty disk slice: close the sub-reader (releases its ref-count grant) and return the
        // empty singleton — caller treats it as "no on-disk content for this checkpoint".
        if (sub.peek() == null) {
            sub.close();
            return CloseableIterator.empty();
        }
        return sub.asIterator();
    }

    /** Closes the root reader (releases its ref-count grant on the spill file). */
    @Override
    public void close() throws IOException {
        rootReader.close();
    }
}
