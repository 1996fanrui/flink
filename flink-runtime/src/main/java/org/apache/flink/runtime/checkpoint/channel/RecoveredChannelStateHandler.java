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

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;
import static org.apache.flink.util.Preconditions.checkArgument;
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
    private final InputGate[] inputGates;

    private final InflightDataRescalingDescriptor channelMapping;

    private final Map<InputChannelInfo, RecoveredInputChannel> rescaledChannels = new HashMap<>();
    private final Map<Integer, RescaleMappings> oldToNewMappings = new HashMap<>();

    /**
     * Optional filtering handler for filtering recovered buffers. When non-null, filtering is
     * performed during recovery in the channel-state-unspilling thread.
     */
    @Nullable private final ChannelStateFilteringHandler filteringHandler;

    /** Network buffer memory segment size in bytes. Used to size the reusable pre-filter buffer. */
    private final int memorySegmentSize;

    /**
     * Optional list of root tmp directories used to locate the spill-file base directory in
     * filtering mode. Falls back to the JVM's {@code java.io.tmpdir} when {@code null} or empty —
     * the spill file lives only for the filter phase so any writable location is acceptable.
     */
    @Nullable private final String[] spillTmpDirectories;

    /**
     * Reusable heap memory segment backing the pre-filter buffer in filtering mode. Lazily
     * allocated on the first {@link #getPreFilterBuffer} call, reused for every subsequent call,
     * and freed in {@link #close()}.
     *
     * <p>Reuse is safe because at most one pre-filter buffer is in flight per task at any moment.
     * This invariant is enforced at runtime by {@link #preFilterBufferInUse}.
     */
    @Nullable private MemorySegment preFilterSegment;

    /**
     * Tracks whether {@link #preFilterSegment} is currently wrapped by a live {@link Buffer} that
     * has not yet been recycled. Flipped to {@code true} when a new buffer is issued, and flipped
     * back to {@code false} by the custom {@link BufferRecycler} when the buffer is recycled.
     */
    private boolean preFilterBufferInUse;

    /**
     * Lazily constructed on the first filter-phase call so that the buffers backing the accumulator
     * can be drawn from a real {@link RecoveredInputChannel}'s pool. {@code null} on the filter-off
     * path. Stays alive for the duration of the filter phase; closed (and the underlying {@link
     * SpillFile} frozen) before {@link #close()} signals filtering completion.
     */
    @Nullable private SpillFileWriter spillFileWriter;

    /** Frozen handle to the produced {@link SpillFile}, populated by {@link #close()}. */
    @Nullable private SpillFile producedSpillFile;

    InputChannelRecoveredStateHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            @Nullable ChannelStateFilteringHandler filteringHandler,
            int memorySegmentSize) {
        this(inputGates, channelMapping, filteringHandler, memorySegmentSize, null);
    }

    InputChannelRecoveredStateHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            @Nullable ChannelStateFilteringHandler filteringHandler,
            int memorySegmentSize,
            @Nullable String[] spillTmpDirectories) {
        this.inputGates = inputGates;
        this.channelMapping = channelMapping;
        this.filteringHandler = filteringHandler;
        checkArgument(
                memorySegmentSize > 0, "memorySegmentSize must be positive: %s", memorySegmentSize);
        this.memorySegmentSize = memorySegmentSize;
        this.spillTmpDirectories = spillTmpDirectories;
    }

    @Override
    public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        if (filteringHandler != null) {
            return getPreFilterBuffer();
        }
        // Non-filtering mode: use existing network buffer pool allocation.
        RecoveredInputChannel channel = getMappedChannels(channelInfo);
        Buffer buffer = channel.requestBufferBlocking();
        return new BufferWithContext<>(wrap(buffer), buffer);
    }

    /**
     * Allocates a pre-filter buffer from a reusable heap segment (isolated from the Network Buffer
     * Pool) in filtering mode.
     *
     * <p>Memory management: a single {@link MemorySegment} per task is lazily allocated on first
     * invocation and reused across every subsequent call. The custom {@link BufferRecycler} does
     * not free the segment — it only flips {@link #preFilterBufferInUse} back to {@code false} so
     * the next call can reuse it. The segment itself is freed in {@link #close()}.
     *
     * <p>Runtime invariant check: the one-at-a-time invariant on pre-filter buffers is guaranteed
     * by Flink's serial recovery loop and the deserializer's ownership contract. This method
     * asserts the invariant before issuing a buffer: if a previously issued buffer has not yet been
     * recycled, it throws {@link IllegalStateException} so any future regression fails loudly
     * instead of silently corrupting memory.
     */
    private BufferWithContext<Buffer> getPreFilterBuffer() {
        checkState(
                !preFilterBufferInUse,
                "Previous pre-filter buffer has not been recycled. This violates the "
                        + "one-buffer-at-a-time invariant of pre-filter buffers.");

        if (preFilterSegment == null) {
            preFilterSegment = MemorySegmentFactory.allocateUnpooledSegment(memorySegmentSize);
        }
        preFilterBufferInUse = true;

        // The recycler keeps the segment alive for reuse; only flips the in-use flag.
        BufferRecycler recycler = segment -> preFilterBufferInUse = false;
        Buffer buffer = new NetworkBuffer(preFilterSegment, recycler);
        return new BufferWithContext<>(wrap(buffer), buffer);
    }

    @VisibleForTesting
    boolean isPreFilterBufferInUse() {
        return preFilterBufferInUse;
    }

    @VisibleForTesting
    @Nullable
    MemorySegment getPreFilterSegmentForTesting() {
        return preFilterSegment;
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
                    recoverWithFiltering(
                            channel, channelInfo, oldSubtaskIndex, buffer.retainBuffer());
                } else {
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
        }
    }

    private void recoverWithFiltering(
            RecoveredInputChannel channel,
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            Buffer retainedBuffer)
            throws IOException, InterruptedException {
        checkState(filteringHandler != null, "filtering handler not set.");
        SpillFileWriter writer = ensureSpillFileWriter(channel);
        FilteredBufferWriter accumulator = writer.getAccumulator();

        List<Buffer> filteredBuffers =
                filteringHandler.filterAndRewrite(
                        channelInfo.getGateIdx(),
                        oldSubtaskIndex,
                        channelInfo.getInputChannelIdx(),
                        retainedBuffer,
                        accumulator::getPrefilterBuffer);

        int i = 0;
        try {
            for (; i < filteredBuffers.size(); i++) {
                Buffer filtered = filteredBuffers.get(i);
                try {
                    writer.write(channelInfo, filtered);
                } finally {
                    // The accumulator copies bytes into its post-filter buffer, so the supplier-
                    // owned filtered buffer can be recycled immediately after each write — keeping
                    // the supplier's buffer pool free for the next allocation.
                    filtered.recycleBuffer();
                }
            }
        } catch (Throwable t) {
            for (int j = i; j < filteredBuffers.size(); j++) {
                filteredBuffers.get(j).recycleBuffer();
            }
            throw t;
        }
    }

    /**
     * Heap-backed segments owned by this handler, retained across the filter phase so that the
     * pre-filter and post-filter buffers can survive intermediate {@code recycleBuffer()} calls
     * from the filter without returning memory to the network pool. Freed in {@link #close()}.
     */
    @Nullable private MemorySegment filterPrefilterSegment;

    @Nullable private MemorySegment filterPostfilterSegment;

    /**
     * Lazily constructs the spill-file pipeline on the first filter call. Buffers backing the
     * accumulator are wrapped over handler-owned heap segments with no-op recyclers so that any
     * intermediate {@code recycleBuffer()} (e.g. from {@code filterAndRewrite} on an empty-output
     * path) does not return memory to the network pool — the segments live for the entire filter
     * phase.
     *
     * <p>Phase 4 will switch these allocations to the {@code RecoveredChannelBufferRequester};
     * Phase 3 uses self-managed heap segments so that the bounded "2 buffers per task" property
     * holds without depending on Phase 4 plumbing.
     */
    private SpillFileWriter ensureSpillFileWriter(RecoveredInputChannel channel)
            throws IOException, InterruptedException {
        if (spillFileWriter != null) {
            return spillFileWriter;
        }
        Path baseDir = resolveSpillBaseDir();
        SpillFile spillFile = new SpillFile(baseDir);

        filterPrefilterSegment = MemorySegmentFactory.allocateUnpooledSegment(memorySegmentSize);
        filterPostfilterSegment = MemorySegmentFactory.allocateUnpooledSegment(memorySegmentSize);
        BufferRecycler resetOnlyRecycler =
                segment -> {
                    // No-op: handler retains ownership of the heap segment for the duration of
                    // the filter phase. The segment is freed in close().
                };
        Buffer prefilter = new NetworkBuffer(filterPrefilterSegment, resetOnlyRecycler);
        Buffer initialPostfilter = new NetworkBuffer(filterPostfilterSegment, resetOnlyRecycler);

        FilteredBufferWriter accumulator =
                new FilteredBufferWriter(
                        spillFile,
                        prefilter,
                        initialPostfilter,
                        () -> {
                            // Phase 3 cap: only one post-filter buffer is allocated; if the
                            // accumulator ever needs to rotate, it indicates filter output exceeds
                            // a single buffer worth of data, which Phase 4 will support via the
                            // RecoveredChannelBufferRequester. Returning the same segment wrapped
                            // in a fresh NetworkBuffer keeps the bound at two buffers total while
                            // preserving the rotate-and-flush semantics inside the accumulator.
                            return new NetworkBuffer(filterPostfilterSegment, resetOnlyRecycler);
                        });
        spillFileWriter = new SpillFileWriter(spillFile, accumulator);
        return spillFileWriter;
    }

    private Path resolveSpillBaseDir() throws IOException {
        String root;
        if (spillTmpDirectories != null && spillTmpDirectories.length > 0) {
            root = spillTmpDirectories[0];
        } else {
            root = System.getProperty("java.io.tmpdir");
        }
        // A fresh subdirectory per handler instance keeps concurrent recoveries (and re-recoveries
        // on the same JVM) isolated. The directory is purposely *not* deleted on close — drain
        // consumes the produced files; deletion is governed by a ref-counted lifecycle introduced
        // in a later phase.
        return Files.createTempDirectory(Paths.get(root), "flink-channel-spill-");
    }

    @VisibleForTesting
    @Nullable
    SpillFile getProducedSpillFileForTesting() {
        return producedSpillFile;
    }

    /**
     * Test-only accessor for the SpillFile currently held by the active {@link SpillFileWriter}.
     * Returns {@code null} on the filter-off path or before the first filter call. Distinct from
     * {@link #getProducedSpillFileForTesting()} which is populated only after {@link #close()}.
     */
    @VisibleForTesting
    @Nullable
    SpillFile peekActiveSpillFileForTesting() {
        return spillFileWriter == null ? null : spillFileWriter.getSpillFile();
    }

    @Override
    public void close() throws IOException {
        // Filter-phase spill file must be frozen before any channel sees finishReadRecoveredState
        // — the latter completes bufferFilteringCompleteFuture on every channel, and Phase 4
        // drain is allowed to assume the spill file is closed by the time that future fires.
        if (spillFileWriter != null) {
            SpillFile produced = spillFileWriter.getSpillFile();
            try {
                spillFileWriter.close();
            } finally {
                producedSpillFile = produced;
                spillFileWriter = null;
            }
        }
        if (filterPrefilterSegment != null) {
            filterPrefilterSegment.free();
            filterPrefilterSegment = null;
        }
        if (filterPostfilterSegment != null) {
            filterPostfilterSegment.free();
            filterPostfilterSegment = null;
        }
        // note that we need to finish all RecoveredInputChannels, not just those with state
        for (final InputGate inputGate : inputGates) {
            inputGate.finishReadRecoveredState();
        }
        if (preFilterSegment != null) {
            preFilterSegment.free();
            preFilterSegment = null;
            preFilterBufferInUse = false;
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
