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
 * Drains a {@link SpillFile} into recovered-buffer queues and snapshots remaining entries when a
 * checkpoint fires during recovery.
 *
 * <p>The private lock pairs channel delivery with root-reader advancement, and also protects
 * checkpoint snapshot creation plus sentinel insertion. Buffer allocation and disk reads stay
 * outside that lock.
 */
@Internal
public final class SpillFileDrainer implements RecoveryCheckpointTrigger, Closeable {

    private final SpillFileReader rootReader;

    private final CompletableFuture<ResolvedChannels> resolvedChannelsFuture;

    private final Object lock = new Object();

    private boolean drainFinished;

    public SpillFileDrainer(
            SpillFile spillFile, CompletableFuture<List<RecoverableInputChannel>> channelsFuture) {
        this.rootReader = spillFile.reader();
        this.resolvedChannelsFuture = checkNotNull(channelsFuture).thenApply(ResolvedChannels::new);
    }

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

    public void drain() throws IOException, InterruptedException {
        ResolvedChannels channels = resolvedChannelsFuture.join();
        SpillFileReader.Chunk chunk;
        while ((chunk = rootReader.peek()) != null) {
            RecoverableInputChannel ch = channels.channelByInfo.get(chunk.channelInfo);
            if (ch == null) {
                throw new IllegalStateException(
                        "Drain: no physical channel found for " + chunk.channelInfo);
            }

            Buffer buf = ch.requestRecoveryBufferBlocking();
            buf.getMemorySegment().put(buf.getMemorySegmentOffset(), chunk.data, 0, chunk.length);
            buf.setSize(chunk.length);

            synchronized (lock) {
                ch.onRecoveredStateBuffer(buf);
                rootReader.advance();
            }
        }

        synchronized (lock) {
            drainFinished = true;
        }

        for (RecoverableInputChannel ch : channels.allChannels) {
            ch.finishRecoveredBufferDelivery();
        }
    }

    @Override
    public CloseableIterator<SpillFileReader.Chunk> snapshotAndInsertBarriers(long checkpointId)
            throws IOException {
        ResolvedChannels channels = resolvedChannelsFuture.join();

        SpillFileReader sub;
        synchronized (lock) {
            for (RecoverableInputChannel ch : channels.allChannels) {
                ch.insertRecoveryCheckpointBarrierIfInRecovery(checkpointId);
            }

            if (drainFinished) {
                return CloseableIterator.empty();
            }

            sub = rootReader.snapshot();
        }

        if (sub.peek() == null) {
            sub.close();
            return CloseableIterator.empty();
        }
        return sub.asIterator();
    }

    @Override
    public void close() throws IOException {
        rootReader.close();
    }
}
