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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;
import org.apache.flink.runtime.memory.MemoryManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;
import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrapWithoutRecycle;
import static org.apache.flink.util.Preconditions.checkState;

interface RecoveredChannelStateHandler<Info, Context> extends AutoCloseable {
    class BufferWithContext<Context> {
        final ChannelStateByteBuffer buffer;
        final Context context;

        BufferWithContext(ChannelStateByteBuffer buffer, Context context) {
            this.buffer = buffer;
            this.context = context;
        }

        public void close() {
            buffer.close();
        }
    }

    BufferWithContext<Context> getBuffer(Info info) throws IOException, InterruptedException;

    /**
     * Recover the data from buffer. This method is taking over the ownership of the
     * bufferWithContext and is fully responsible for cleaning it up both on the happy path and in
     * case of an error.
     */
    void recover(Info info, int oldSubtaskIndex, BufferWithContext<Context> bufferWithContext)
            throws IOException, InterruptedException;
}

class InputChannelRecoveredStateHandler
        implements RecoveredChannelStateHandler<InputChannelInfo, Buffer> {

    private static final Logger LOG =
            LoggerFactory.getLogger(InputChannelRecoveredStateHandler.class);

    private final InputGate[] inputGates;

    private final InflightDataRescalingDescriptor channelMapping;

    private final Map<InputChannelInfo, RecoveredInputChannel> rescaledChannels = new HashMap<>();
    private final Map<Integer, RescaleMappings> oldToNewMappings = new HashMap<>();

    /**
     * Optional filtering handler for filtering recovered buffers. When non-null, filtering is
     * performed during recovery in the channel-state-unspilling thread.
     */
    @Nullable private final ChannelStateFilteringHandler filteringHandler;

    /** Spilling managers per gate, lazily created. Only used when filtering is enabled. */
    @Nullable private SpillingBufferManager[] spillingManagers;

    /** Maximum number of Heap Buffers per Gate (~160KB at 32KB each). */
    static final int MAX_HEAP_BUFFERS_PER_GATE = 5;

    /** Tracks active Heap Buffer count per gate to prevent unbounded heap growth. */
    private final int[] heapBufferCounts;

    private final String[] spillDirs;
    private final String attemptId;

    InputChannelRecoveredStateHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            @Nullable ChannelStateFilteringHandler filteringHandler) {
        this(inputGates, channelMapping, filteringHandler, new String[0], "default");
    }

    InputChannelRecoveredStateHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            @Nullable ChannelStateFilteringHandler filteringHandler,
            String[] spillDirs,
            String attemptId) {
        this.inputGates = inputGates;
        this.channelMapping = channelMapping;
        this.filteringHandler = filteringHandler;
        this.spillDirs = spillDirs;
        this.attemptId = attemptId;
        this.heapBufferCounts = new int[inputGates.length];
    }

    @Override
    public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        RecoveredInputChannel channel = getMappedChannels(channelInfo);

        if (filteringHandler != null) {
            // Filtering mode: allocate Heap Buffer (Source Buffer) to avoid competing
            // with Network Buffer Pool. Enforce per-gate limit to prevent unbounded growth.
            int gateIdx = channelInfo.getGateIdx();
            checkState(
                    heapBufferCounts[gateIdx] < MAX_HEAP_BUFFERS_PER_GATE,
                    "Heap buffer limit (%s) exceeded for gate %s. "
                            + "This indicates a bug: buffers are not being released properly.",
                    MAX_HEAP_BUFFERS_PER_GATE,
                    gateIdx);
            heapBufferCounts[gateIdx]++;

            MemorySegment segment =
                    MemorySegmentFactory.allocateUnpooledSegment(MemoryManager.DEFAULT_PAGE_SIZE);
            Buffer buffer =
                    new NetworkBuffer(
                            segment,
                            FreeingBufferRecycler.INSTANCE,
                            Buffer.DataType.DATA_BUFFER,
                            0);
            return new BufferWithContext<>(wrapWithoutRecycle(buffer), buffer);
        } else {
            // Non-filtering mode: use original behavior
            Buffer buffer = channel.requestBuffer();
            if (buffer == null) {
                // Heap buffer fallback when Network Buffer Pool is exhausted during
                // unaligned recovery. Avoids deadlock by not competing with the pool.
                MemorySegment segment =
                        MemorySegmentFactory.allocateUnpooledSegment(
                                MemoryManager.DEFAULT_PAGE_SIZE);
                buffer =
                        new NetworkBuffer(
                                segment,
                                FreeingBufferRecycler.INSTANCE,
                                Buffer.DataType.DATA_BUFFER,
                                0);
            }
            return new BufferWithContext<>(wrap(buffer), buffer);
        }
    }

    @Override
    public void recover(
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            BufferWithContext<Buffer> bufferWithContext)
            throws IOException, InterruptedException {
        Buffer buffer = bufferWithContext.context;
        try {
            if (buffer.readableBytes() > 0) {
                RecoveredInputChannel channel = getMappedChannels(channelInfo);

                if (filteringHandler != null) {
                    // Filtering mode: filter records and route through three-path logic
                    recoverWithFiltering(channel, channelInfo, oldSubtaskIndex, buffer);
                } else {
                    // Non-filtering mode: pass through original buffer with descriptor
                    channel.onRecoveredStateBuffer(
                            EventSerializer.toBuffer(
                                    new SubtaskConnectionDescriptor(
                                            oldSubtaskIndex, channelInfo.getInputChannelIdx()),
                                    false));
                    channel.onRecoveredStateBuffer(buffer.retainBuffer());
                }
            }
        } finally {
            buffer.recycleBuffer();
            // Release Heap Buffer count for filtering mode
            if (filteringHandler != null) {
                heapBufferCounts[channelInfo.getGateIdx()]--;
            }
        }
    }

    private void recoverWithFiltering(
            RecoveredInputChannel channel,
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            Buffer buffer)
            throws IOException, InterruptedException {
        checkState(filteringHandler != null, "filtering handler not set.");

        // Extra retain: filterAndRewrite consumes one ref, caller's finally releases another.
        buffer.retainBuffer();

        List<Buffer> filteredBuffers;
        try {
            // Filtered buffers use Network Buffer Pool. Blocking is safe because
            // Source Buffer uses Heap (no pool competition), so Task consuming
            // buffers will eventually free pool space.
            filteredBuffers =
                    filteringHandler.filterAndRewrite(
                            channelInfo.getGateIdx(),
                            oldSubtaskIndex,
                            channelInfo.getInputChannelIdx(),
                            buffer,
                            () -> requestBufferBlocking(channel));
        } catch (Throwable t) {
            // filterAndRewrite didn't consume the buffer, release the extra ref.
            buffer.recycleBuffer();
            throw t;
        }

        // Route each filtered buffer (Network Buffer) through three-path logic
        SpillingBufferManager manager = getOrCreateSpillingManager(channelInfo.getGateIdx());
        for (Buffer filteredBuffer : filteredBuffers) {
            try {
                routeFilteredBuffer(channel, channelInfo, oldSubtaskIndex, filteredBuffer, manager);
            } finally {
                filteredBuffer.recycleBuffer();
            }
        }
    }

    /**
     * Routes a filtered buffer through the three-path scheduling logic. Since filtered buffers are
     * already Network Buffers (allocated via blocking pool request), P1 can deliver directly
     * without copying.
     *
     * <ul>
     *   <li>P1 (Memory): no disk data -> deliver filtered buffer directly (already a Network
     *       Buffer)
     *   <li>P3 (Replay): disk has data -> spill current to preserve FIFO, then replay disk data
     * </ul>
     */
    private void routeFilteredBuffer(
            RecoveredInputChannel channel,
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            Buffer filteredBuffer,
            SpillingBufferManager manager)
            throws IOException, InterruptedException {

        if (!manager.hasDiskData()) {
            // P1: Memory Path - filtered buffer is already a Network Buffer, deliver directly.
            // Retain because the caller's finally block will recycle.
            deliverBuffer(
                    channel,
                    oldSubtaskIndex,
                    channelInfo.getInputChannelIdx(),
                    filteredBuffer.retainBuffer());
        } else {
            // P3: Replay Path - spill current data to preserve FIFO, then replay disk data
            manager.spillBuffer(filteredBuffer, oldSubtaskIndex, channelInfo.getInputChannelIdx());

            // Try to get a network buffer for replaying disk data
            Buffer networkBuffer = channel.requestBuffer();
            if (networkBuffer != null) {
                SpillingBufferManager.ReplayResult replayed = manager.replayToBuffer(networkBuffer);
                if (replayed != null) {
                    deliverBuffer(
                            channel,
                            replayed.oldSubtaskIndex,
                            replayed.oldChannelIndex,
                            replayed.buffer);
                } else {
                    networkBuffer.recycleBuffer();
                }
            }
            // If no network buffer available, disk data stays for Phase 2 drain
        }
    }

    /**
     * Phase 2: Drain remaining spill data from disk after all S3 data has been read. Blocking
     * buffer requests are safe here because all Source Buffers (heap buffers) have been released,
     * so no deadlock can occur.
     */
    void drainDiskData() throws IOException, InterruptedException {
        if (spillingManagers == null) {
            return;
        }

        for (int gateIdx = 0; gateIdx < spillingManagers.length; gateIdx++) {
            SpillingBufferManager manager = spillingManagers[gateIdx];
            if (manager == null || !manager.hasDiskData()) {
                continue;
            }

            // Get any channel in this gate for requesting buffers.
            // The specific channel doesn't matter because each replayed entry carries
            // its own channel context (oldSubtaskIndex, oldChannelIndex) for delivery.
            RecoveredInputChannel anyChannel = getAnyChannelInGate(gateIdx);

            while (manager.hasDiskData()) {
                Buffer networkBuffer = requestBufferBlocking(anyChannel);
                SpillingBufferManager.ReplayResult replayed = manager.replayToBuffer(networkBuffer);
                if (replayed != null) {
                    // Look up the correct target channel for this replayed entry
                    InputChannelInfo replayedInfo =
                            new InputChannelInfo(gateIdx, replayed.oldChannelIndex);
                    RecoveredInputChannel targetChannel = getMappedChannels(replayedInfo);
                    deliverBuffer(
                            targetChannel,
                            replayed.oldSubtaskIndex,
                            replayed.oldChannelIndex,
                            replayed.buffer);
                } else {
                    // No more data, recycle the unused network buffer
                    networkBuffer.recycleBuffer();
                    break;
                }
            }
        }
    }

    private void deliverBuffer(
            RecoveredInputChannel channel, int oldSubtaskIndex, int oldChannelIndex, Buffer buffer)
            throws IOException {
        channel.onRecoveredStateBuffer(
                EventSerializer.toBuffer(
                        new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex), false));
        channel.onRecoveredStateBuffer(buffer);
    }

    /** Blocking buffer request. Safe because Source Buffers use Heap, not Network Pool. */
    private Buffer requestBufferBlocking(RecoveredInputChannel channel)
            throws IOException, InterruptedException {
        Buffer buffer;
        while ((buffer = channel.requestBuffer()) == null) {
            Thread.sleep(1);
        }
        return buffer;
    }

    private RecoveredInputChannel getAnyChannelInGate(int gateIndex) {
        return (RecoveredInputChannel) inputGates[gateIndex].getChannel(0);
    }

    private SpillingBufferManager getOrCreateSpillingManager(int gateIndex) {
        if (spillingManagers == null) {
            spillingManagers = new SpillingBufferManager[inputGates.length];
        }
        if (spillingManagers[gateIndex] == null) {
            String[] dirs =
                    (spillDirs.length > 0)
                            ? spillDirs
                            : new String[] {System.getProperty("java.io.tmpdir")};
            spillingManagers[gateIndex] = new SpillingBufferManager(dirs, attemptId, gateIndex);
        }
        return spillingManagers[gateIndex];
    }

    @Override
    public void close() throws IOException {
        try {
            // Note: we need to finish all RecoveredInputChannels, not just those with state
            for (final InputGate inputGate : inputGates) {
                inputGate.finishReadRecoveredState();
            }
        } finally {
            closeSpillingManagers();
        }
    }

    private void closeSpillingManagers() {
        if (spillingManagers != null) {
            for (SpillingBufferManager manager : spillingManagers) {
                if (manager != null) {
                    try {
                        manager.close();
                    } catch (IOException e) {
                        LOG.warn("Failed to close SpillingBufferManager", e);
                    }
                }
            }
        }
    }

    private RecoveredInputChannel getChannel(int gateIndex, int subPartitionIndex) {
        final InputChannel inputChannel = inputGates[gateIndex].getChannel(subPartitionIndex);
        if (!(inputChannel instanceof RecoveredInputChannel)) {
            throw new IllegalStateException(
                    "Cannot restore state to a non-recovered input channel: " + inputChannel);
        }
        return (RecoveredInputChannel) inputChannel;
    }

    private RecoveredInputChannel getMappedChannels(InputChannelInfo channelInfo) {
        return rescaledChannels.computeIfAbsent(channelInfo, this::calculateMapping);
    }

    @Nonnull
    private RecoveredInputChannel calculateMapping(InputChannelInfo info) {
        final RescaleMappings oldToNewMapping =
                oldToNewMappings.computeIfAbsent(
                        info.getGateIdx(), idx -> channelMapping.getChannelMapping(idx).invert());
        int[] mappedIndexes = oldToNewMapping.getMappedIndexes(info.getInputChannelIdx());
        checkState(
                mappedIndexes.length == 1,
                "One buffer is only distributed to one target InputChannel since "
                        + "one buffer is expected to be processed once by the same task.");
        return getChannel(info.getGateIdx(), mappedIndexes[0]);
    }
}

class ResultSubpartitionRecoveredStateHandler
        implements RecoveredChannelStateHandler<ResultSubpartitionInfo, BufferBuilder> {

    private final ResultPartitionWriter[] writers;
    private final boolean notifyAndBlockOnCompletion;
    private final ResultSubpartitionDistributor resultSubpartitionDistributor;

    ResultSubpartitionRecoveredStateHandler(
            ResultPartitionWriter[] writers,
            boolean notifyAndBlockOnCompletion,
            InflightDataRescalingDescriptor channelMapping) {
        this.writers = writers;
        this.resultSubpartitionDistributor =
                new ResultSubpartitionDistributor(channelMapping) {
                    /**
                     * Override the getSubpartitionInfo to perform type checking on the
                     * ResultPartitionWriter.
                     */
                    @Override
                    ResultSubpartitionInfo getSubpartitionInfo(
                            int partitionIndex, int subPartitionIdx) {
                        CheckpointedResultPartition writer =
                                getCheckpointedResultPartition(partitionIndex);
                        return writer.getCheckpointedSubpartitionInfo(subPartitionIdx);
                    }
                };
        this.notifyAndBlockOnCompletion = notifyAndBlockOnCompletion;
    }

    @Override
    public BufferWithContext<BufferBuilder> getBuffer(ResultSubpartitionInfo subpartitionInfo)
            throws IOException, InterruptedException {
        // request the buffer from any mapped subpartition as they all will receive the same buffer
        BufferBuilder bufferBuilder =
                getCheckpointedResultPartition(subpartitionInfo.getPartitionIdx())
                        .requestBufferBuilderBlocking();
        return new BufferWithContext<>(wrap(bufferBuilder), bufferBuilder);
    }

    @Override
    public void recover(
            ResultSubpartitionInfo subpartitionInfo,
            int oldSubtaskIndex,
            BufferWithContext<BufferBuilder> bufferWithContext)
            throws IOException, InterruptedException {
        try (BufferBuilder bufferBuilder = bufferWithContext.context;
                BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumerFromBeginning()) {
            bufferBuilder.finish();
            if (!bufferConsumer.isDataAvailable()) {
                return;
            }
            final List<ResultSubpartitionInfo> mappedSubpartitions =
                    resultSubpartitionDistributor.getMappedSubpartitions(subpartitionInfo);
            CheckpointedResultPartition checkpointedResultPartition =
                    getCheckpointedResultPartition(subpartitionInfo.getPartitionIdx());
            for (final ResultSubpartitionInfo mappedSubpartition : mappedSubpartitions) {
                // channel selector is created from the downstream's point of view: the
                // subtask of downstream = subpartition index of recovered buffer
                final SubtaskConnectionDescriptor channelSelector =
                        new SubtaskConnectionDescriptor(
                                subpartitionInfo.getSubPartitionIdx(), oldSubtaskIndex);
                checkpointedResultPartition.addRecovered(
                        mappedSubpartition.getSubPartitionIdx(),
                        EventSerializer.toBufferConsumer(channelSelector, false));
                checkpointedResultPartition.addRecovered(
                        mappedSubpartition.getSubPartitionIdx(), bufferConsumer.copy());
            }
        }
    }

    private CheckpointedResultPartition getCheckpointedResultPartition(int partitionIndex) {
        ResultPartitionWriter writer = writers[partitionIndex];
        if (!(writer instanceof CheckpointedResultPartition)) {
            throw new IllegalStateException(
                    "Cannot restore state to a non-checkpointable partition type: " + writer);
        }
        return (CheckpointedResultPartition) writer;
    }

    @Override
    public void close() throws IOException {
        for (ResultPartitionWriter writer : writers) {
            if (writer instanceof CheckpointedResultPartition) {
                ((CheckpointedResultPartition) writer)
                        .finishReadRecoveredState(notifyAndBlockOnCompletion);
            }
        }
    }
}
