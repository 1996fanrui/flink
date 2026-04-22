/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An input channel reads recovered state from previous unaligned checkpoint snapshots. */
public abstract class RecoveredInputChannel extends InputChannel implements ChannelStateHolder {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveredInputChannel.class);

    private final RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl();
    private final CompletableFuture<?> stateConsumedFuture = new CompletableFuture<>();
    protected final BufferManager bufferManager;

    /**
     * Future that completes when recovered buffers have been filtered for this channel. This
     * completes before stateConsumedFuture, enabling earlier RUNNING state transition when
     * unaligned checkpoint during recovery is enabled.
     */
    private final CompletableFuture<Void> bufferFilteringCompleteFuture = new CompletableFuture<>();

    private final AtomicBoolean isReleased = new AtomicBoolean(false);

    protected ChannelStateWriter channelStateWriter;

    /**
     * The buffer number of recovered buffers. Starts at MIN_VALUE to have no collisions with actual
     * buffer numbers.
     */
    private int sequenceNumber = Integer.MIN_VALUE;

    protected final int networkBuffersPerChannel;
    private boolean exclusiveBuffersAssigned;

    private long lastStoppedCheckpointId = -1;

    RecoveredInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn,
            int networkBuffersPerChannel) {
        super(
                inputGate,
                channelIndex,
                partitionId,
                consumedSubpartitionIndexSet,
                initialBackoff,
                maxBackoff,
                numBytesIn,
                numBuffersIn);

        bufferManager = new BufferManager(inputGate.getMemorySegmentProvider(), this, 0);
        this.networkBuffersPerChannel = networkBuffersPerChannel;
        store.setNotificationCallback(this::notifyChannelNonEmpty);
    }

    /**
     * Returns the store for recovered buffers. Used for store transfer during channel conversion,
     * by {@link org.apache.flink.runtime.checkpoint.channel.RecoveredChannelStateHandler} to add
     * recovered buffers directly, and for FilteredBufferDispatcher integration during filtering.
     */
    public RecoveredBufferStoreImpl getStore() {
        return store;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        checkState(this.channelStateWriter == null, "Already initialized");
        this.channelStateWriter = checkNotNull(channelStateWriter);
    }

    /**
     * Returns the ChannelStateWriter assigned to this channel. Used by FilteredBufferDispatcher to
     * obtain the writer for phase2 disk checkpoint without threading it through every call site.
     *
     * <p>Must be called after {@link #setChannelStateWriter} has been invoked (i.e., after channel
     * state writer injection during task initialization).
     */
    public ChannelStateWriter getChannelStateWriter() {
        return checkNotNull(channelStateWriter, "ChannelStateWriter has not been set yet");
    }

    public final InputChannel toInputChannel() throws IOException {
        Preconditions.checkState(
                bufferFilteringCompleteFuture.isDone(), "buffer filtering is not complete");
        if (!inputGate.isCheckpointingDuringRecoveryEnabled()) {
            Preconditions.checkState(
                    stateConsumedFuture.isDone(), "recovered state is not fully consumed");
        }

        // Pass the store reference to the physical channel for continued consumption.
        final InputChannel inputChannel = toInputChannelInternal(store);
        inputChannel.checkpointStopped(lastStoppedCheckpointId);
        return inputChannel;
    }

    @Override
    public void checkpointStopped(long checkpointId) {
        this.lastStoppedCheckpointId = checkpointId;
    }

    /**
     * Creates the physical InputChannel from this recovered channel.
     *
     * @param recoveredStore the store containing recovered buffers that have been filtered but not
     *     yet consumed by the Task. The store reference is passed to the physical channel for
     *     continued consumption.
     * @return the physical InputChannel (LocalInputChannel or RemoteInputChannel)
     */
    protected abstract InputChannel toInputChannelInternal(RecoveredBufferStoreImpl recoveredStore)
            throws IOException;

    /**
     * Returns the future that completes when buffer filtering is complete. This future completes
     * before stateConsumedFuture, at the point when finishReadRecoveredState() is called.
     */
    CompletableFuture<Void> getBufferFilteringCompleteFuture() {
        return bufferFilteringCompleteFuture;
    }

    CompletableFuture<?> getStateConsumedFuture() {
        return stateConsumedFuture;
    }

    public void finishReadRecoveredState() throws IOException {
        // Adding the event and completing the future must be atomic under the store lock.
        // Without this, either ordering has a race:
        // - event first: task thread consumes EndOfInputChannelStateEvent, which completes
        //   stateConsumedFuture. When checkpointing during recovery is disabled,
        //   stateConsumedFuture triggers requestPartitions -> toInputChannel(), which
        //   fails because bufferFilteringCompleteFuture is not yet done.
        // - future first: toInputChannel() passes the store before the event is added,
        //   losing the EndOfInputChannelStateEvent.
        // RecoveredBufferStoreImpl.addBuffer is synchronized, so we synchronize on the store
        // to guarantee atomicity of adding the event and completing the future.
        synchronized (store) {
            store.addBuffer(EventSerializer.toBuffer(EndOfInputChannelStateEvent.INSTANCE, false));
            bufferFilteringCompleteFuture.complete(null);
        }
        bufferManager.releaseFloatingBuffers();
        LOG.debug("{}/{} finished recovering input.", inputGate.getOwningTaskName(), channelInfo);
    }

    @Nullable
    private BufferAndAvailability getNextRecoveredStateBuffer() throws IOException {
        checkState(!isReleased.get(), "Trying to read from released RecoveredInputChannel");
        final Buffer next = store.tryTake();

        if (next == null) {
            return null;
        } else if (isEndOfInputChannelStateEvent(next)) {
            Preconditions.checkState(
                    bufferFilteringCompleteFuture.isDone(), "buffer filtering is not complete");
            stateConsumedFuture.complete(null);
            return null;
        } else {
            final Buffer.DataType nextDataType = store.peekNextDataType();
            return new BufferAndAvailability(next, nextDataType, 0, sequenceNumber++);
        }
    }

    private boolean isEndOfInputChannelStateEvent(Buffer buffer) throws IOException {
        if (buffer.isBuffer()) {
            return false;
        }

        AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
        buffer.setReaderIndex(0);
        return event.getClass() == EndOfInputChannelStateEvent.class;
    }

    @Override
    protected int peekNextBufferSubpartitionIdInternal() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer() throws IOException {
        checkError();
        return Optional.ofNullable(getNextRecoveredStateBuffer());
    }

    @Override
    int getBuffersInUseCount() {
        return store.size();
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("RecoveredInputChannel should never be blocked.");
    }

    @Override
    public void acknowledgeAllRecordsProcessed() throws IOException {
        // We should not receive the EndOfUserRecordsEvent since it would
        // turn into real channel before requesting partition. Besides,
        // the event would not be persist in the unaligned checkpoint
        // case, thus this also cannot happen during restoring state.
        throw new UnsupportedOperationException(
                "RecoveredInputChannel should not need acknowledge all records processed.");
    }

    @Override
    final void requestSubpartitions() {
        throw new UnsupportedOperationException(
                "RecoveredInputChannel should never request partition.");
    }

    @Override
    void sendTaskEvent(TaskEvent event) {
        throw new UnsupportedOperationException(
                "RecoveredInputChannel should never send any task events.");
    }

    @Override
    boolean isReleased() {
        return isReleased.get();
    }

    void releaseAllResources() throws IOException {
        if (isReleased.compareAndSet(false, true)) {
            store.releaseAll();
            bufferManager.releaseAllBuffers(new ArrayDeque<>());
        }
    }

    @VisibleForTesting
    protected int getNumberOfQueuedBuffers() {
        return store.size();
    }

    /**
     * Non-blocking buffer request. Returns a buffer from the pool, or {@code null} if the pool is
     * exhausted.
     */
    @Nullable
    public Buffer requestBuffer() throws IOException {
        if (!exclusiveBuffersAssigned) {
            bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
            exclusiveBuffersAssigned = true;
        }
        return bufferManager.requestBuffer();
    }

    public Buffer requestBufferBlocking() throws InterruptedException, IOException {
        // not in setup to avoid assigning buffers unnecessarily if there is no state
        if (!exclusiveBuffersAssigned) {
            bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
            exclusiveBuffersAssigned = true;
        }
        // Both filtering and non-filtering modes block-wait for a Network Buffer Pool buffer. In
        // filtering mode the pre-filter buffer is heap-allocated separately in the state handler,
        // so this method is only called for post-filter buffers which must come from the pool.
        return bufferManager.requestBufferBlocking();
    }

    @Override
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
    }

    @Override
    void announceBufferSize(int newBufferSize) {
        // Not supported.
    }
}
