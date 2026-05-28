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
import java.nio.ByteBuffer;
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
     * Unpooled heap segment backing the post-filter accumulator. Lazily allocated on the first
     * filter call, reused for the whole filter phase, and freed in {@link #close()}. Sized to one
     * network buffer — the post-filter byte stream is flushed to {@link SpillFile} every time the
     * accumulator fills, so a single buffer is sufficient.
     */
    @Nullable private MemorySegment postFilterSegment;

    /**
     * Lazily constructed on the first filter call. {@code null} when no spill output is produced.
     * Stays alive until {@link #close()} freezes the underlying {@link SpillFile} and publishes the
     * handle.
     */
    @Nullable private SpillFileWriter spillFileWriter;

    /** Frozen handle to the produced {@link SpillFile}, populated by {@link #close()}. */
    @Nullable private SpillFile producedSpillFile;

    /**
     * Drives the {@code recover} dispatch:
     *
     * <ul>
     *   <li>{@code true} + {@link #filteringHandler} != null — rescale path; bytes are filtered and
     *       the surviving records are written to the {@link SpillFile} via the accumulator.
     *   <li>{@code true} + {@link #filteringHandler} == null — non-rescale path; bytes still go to
     *       the {@link SpillFile} (pass-through), giving the drain a single uniform source.
     *   <li>{@code false} — bytes are pushed directly into the {@link RecoveredInputChannel}'s
     *       {@code receivedBuffers} for in-line consumption by the task's mailbox loop.
     * </ul>
     */
    private final boolean checkpointingDuringRecoveryEnabled;

    InputChannelRecoveredStateHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            @Nullable ChannelStateFilteringHandler filteringHandler,
            boolean checkpointingDuringRecoveryEnabled,
            int memorySegmentSize,
            @Nullable String[] spillTmpDirectories) {
        this.inputGates = inputGates;
        this.channelMapping = channelMapping;
        this.filteringHandler = filteringHandler;
        this.checkpointingDuringRecoveryEnabled = checkpointingDuringRecoveryEnabled;
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

                if (checkpointingDuringRecoveryEnabled) {
                    if (filteringHandler != null) {
                        recoverWithFiltering(
                                channel, channelInfo, oldSubtaskIndex, buffer.retainBuffer());
                    } else {
                        recoverPassThroughToSpill(channel.getChannelInfo(), buffer);
                    }
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

    /**
     * Copies the source buffer's raw bytes into the spill-file accumulator unchanged. Used when
     * checkpointing-during-recovery is on but no rescale filtering is needed, so the drain has a
     * uniform source. Copies in chunks bounded by the accumulator's writable capacity; each {@code
     * requestBufferBlocking} call flushes the accumulator when the channel switched or the buffer
     * is full, so a single source buffer may produce multiple {@link SpillFile} entries.
     */
    private void recoverPassThroughToSpill(InputChannelInfo channelInfo, Buffer source)
            throws IOException, InterruptedException {
        FilteredBufferWriter accumulator = ensureSpillFileWriter().getAccumulator();
        ByteBuffer src = source.getNioBufferReadable();
        while (src.hasRemaining()) {
            Buffer dst = accumulator.requestBufferBlocking(channelInfo);
            int writable = dst.getMaxCapacity() - dst.getSize();
            int toCopy = Math.min(writable, src.remaining());
            ByteBuffer slice = src.slice();
            slice.limit(toCopy);
            dst.getMemorySegment().put(dst.getSize(), slice, toCopy);
            dst.setSize(dst.getSize() + toCopy);
            src.position(src.position() + toCopy);
        }
    }

    private void recoverWithFiltering(
            RecoveredInputChannel channel,
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            Buffer retainedBuffer)
            throws IOException, InterruptedException {
        checkState(filteringHandler != null, "filtering handler not set.");
        SpillFileWriter writer = ensureSpillFileWriter();
        FilteredBufferWriter accumulator = writer.getAccumulator();

        // Pass the mapped (post-rescale) channel's InputChannelInfo to the filter chain so each
        // filter-internal bufferSupplier.requestBufferBlocking(...) call tags the accumulator
        // with the NEW channel — switching NEW channels triggers a flush so each spill file
        // entry carries exactly one channel's bytes. The `channelInfo` argument here carries the
        // OLD (pre-rescale) channel index and only matches the physical channel when parallelism
        // is unchanged.
        filteringHandler.filterAndRewrite(
                channelInfo.getGateIdx(),
                oldSubtaskIndex,
                channelInfo.getInputChannelIdx(),
                channel.getChannelInfo(),
                retainedBuffer,
                accumulator);
    }

    /**
     * Lazily constructs the spill-file pipeline on the first filter call. The accumulator's backing
     * memory is an unpooled heap segment owned by the handler (same pattern as {@link
     * #preFilterSegment}), so this method has no dependency on any particular channel's buffer
     * pool. {@code channel switch} and {@code buffer full} are the two flush triggers; the segment
     * is freed in {@link #close()}.
     */
    private SpillFileWriter ensureSpillFileWriter() throws IOException {
        if (spillFileWriter != null) {
            return spillFileWriter;
        }
        Path baseDir = resolveSpillBaseDir();
        SpillFile spillFile = new SpillFile(baseDir);
        // Producer-side ref-count grant. Held by the handler from pipeline construction until
        // the SpillFileReader takes its own grant on construction, at which point StreamTask
        // releases this grant. Without it the SpillFile could be cleaned up between filter end
        // and drain start, since nothing else holds the file alive in that window.
        spillFile.acquire();

        postFilterSegment = MemorySegmentFactory.allocateUnpooledSegment(memorySegmentSize);

        BufferRecycler noOpRecycler =
                segment -> {
                    // No-op: handler retains ownership of the segment for the duration of the
                    // filter phase. The segment is freed in close().
                };
        Buffer outputBuffer = new NetworkBuffer(postFilterSegment, noOpRecycler);

        FilteredBufferWriter accumulator = new FilteredBufferWriter(spillFile, outputBuffer);
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
        // A fresh subdirectory per handler instance isolates concurrent recoveries (and
        // re-recoveries on the same JVM). The directory is intentionally not deleted on close —
        // deletion is driven by the ref-counted lifecycle in SpillFile (acquire/release pairs).
        return Files.createTempDirectory(Paths.get(root), "flink-channel-spill-");
    }

    /**
     * Returns the produced {@link SpillFile} once {@link #close} has run on the filter-on path, or
     * {@code null} otherwise. The drain consumes this handle after filter completes.
     */
    @Nullable
    SpillFile getProducedSpillFile() {
        return producedSpillFile;
    }

    /**
     * Test-only accessor for the SpillFile held by the active {@link SpillFileWriter}. Returns
     * {@code null} on the filter-off path or before the first filter call. Distinct from {@link
     * #getProducedSpillFile()} which is only populated after {@link #close()}.
     */
    @VisibleForTesting
    @Nullable
    SpillFile peekActiveSpillFileForTesting() {
        return spillFileWriter == null ? null : spillFileWriter.getSpillFile();
    }

    @Override
    public void close() throws IOException {
        // Freeze the spill file here so producedSpillFile is available to callers immediately
        // after close. finishReadRecoveredState() is invoked by the caller (StreamTask filter
        // runnable) once it has built the SpillFileReader and published the trigger reference,
        // so the completion future observes a stable trigger.
        if (spillFileWriter != null) {
            SpillFile produced = spillFileWriter.getSpillFile();
            try {
                spillFileWriter.close();
            } finally {
                producedSpillFile = produced;
                spillFileWriter = null;
            }
        }
        if (postFilterSegment != null) {
            postFilterSegment.free();
            postFilterSegment = null;
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
