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
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.FileRegionBuffer;
import org.apache.flink.runtime.io.network.buffer.FullyFilledBuffer;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An input channel, which requests a local subpartition. */
public class LocalInputChannel extends InputChannel
        implements BufferAvailabilityListener, RecoverableInputChannel {

    private static final Logger LOG = LoggerFactory.getLogger(LocalInputChannel.class);

    // ------------------------------------------------------------------------

    private final Object requestLock = new Object();

    /** The local partition manager. */
    private final ResultPartitionManager partitionManager;

    /** Task event dispatcher for backwards events. */
    private final TaskEventPublisher taskEventPublisher;

    /** The consumed subpartition. */
    @Nullable private volatile ResultSubpartitionView subpartitionView;

    private volatile boolean isReleased;

    private final ChannelStatePersister channelStatePersister;

    private final Deque<BufferAndBacklog> toBeConsumedBuffers = new ArrayDeque<>();

    /**
     * Recovery state: buffers delivered by the spill/drain producer, the producer-completion flag,
     * and the recovery-sequence counter. The queue instance itself serves as the monitor for all
     * recovery-state reads/writes on this channel.
     */
    private final RecoveredBufferQueue recoveredQueue;

    /**
     * Whether a priority event (e.g., checkpoint barrier) is pending in {@code subpartitionView}
     * and must be consumed before {@code recoveredQueue}. Volatile because it is written by the
     * network thread and read by the task thread.
     */
    private volatile boolean hasPendingPriorityEvent = false;

    /**
     * Completed once {@link #requestSubpartitions()} successfully publishes {@link
     * #subpartitionView}. Recovered-buffer producers await this before pushing into {@code
     * recoveredQueue}, guaranteeing that any buffer reaching the queue is delivered with the
     * upstream view already visible. Completed exceptionally on release so awaiters unpark instead
     * of deadlocking.
     */
    private final CompletableFuture<Void> upstreamReady = new CompletableFuture<>();

    public LocalInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
            ResultPartitionManager partitionManager,
            TaskEventPublisher taskEventPublisher,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn,
            ChannelStateWriter stateWriter) {

        super(
                inputGate,
                channelIndex,
                partitionId,
                consumedSubpartitionIndexSet,
                initialBackoff,
                maxBackoff,
                numBytesIn,
                numBuffersIn);

        this.partitionManager = checkNotNull(partitionManager);
        this.taskEventPublisher = checkNotNull(taskEventPublisher);
        this.channelStatePersister =
                new ChannelStatePersister(checkNotNull(stateWriter), getChannelInfo());
        // When the gate has no final-drain to perform, no producer will ever push into
        // recoveredQueue, so start it with allDelivered=true (isInRecovery=false). Otherwise the
        // channel enters in-recovery and finishRecoveredBufferDelivery() flips allDelivered later.
        this.recoveredQueue =
                new RecoveredBufferQueue(getChannelInfo(), !inputGate.isFinalDrainEnabled());
    }

    // ------------------------------------------------------------------------
    // RecoverableInputChannel implementation
    // ------------------------------------------------------------------------

    /**
     * Appends {@code buffer} to {@code recoveredQueue} and wakes the consumer when the queue
     * transitions from empty to non-empty. If the channel was released before we entered the
     * synchronized block, the buffer is recycled silently. The upstream-ready wait is performed
     * externally by the caller (see {@link #awaitUpstreamReady}).
     */
    @Override
    public void onRecoveredStateBuffer(Buffer buffer) {
        boolean wasEmpty;
        synchronized (recoveredQueue) {
            if (isReleased) {
                buffer.recycleBuffer();
                return;
            }
            wasEmpty = recoveredQueue.offer(buffer);
        }
        if (wasEmpty) {
            notifyChannelNonEmpty();
        }
    }

    /**
     * Flips the producer-completion flag to true and wakes the consumer so it re-checks the
     * channel. No sentinel buffer is pushed; the wake-up alone is enough to flush any upstream
     * {@code notifyDataAvailable} that was absorbed while the channel was in-recovery.
     */
    @Override
    public void finishRecoveredBufferDelivery() {
        synchronized (recoveredQueue) {
            recoveredQueue.finish();
        }
        notifyChannelNonEmpty();
    }

    @Override
    public void awaitUpstreamReady() {
        upstreamReady.join();
    }

    @Override
    public boolean isInRecovery() {
        synchronized (recoveredQueue) {
            return recoveredQueue.isInRecovery();
        }
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        try {
            List<Buffer> toPersist;
            synchronized (recoveredQueue) {
                if (recoveredQueue.isInRecovery()) {
                    toPersist = recoveredQueue.collectPreRecoveryBarrier(barrier.getId());
                } else {
                    toPersist = Collections.emptyList();
                }
            }
            channelStatePersister.startPersisting(barrier.getId(), toPersist);
        } catch (IOException e) {
            throw new CheckpointException(
                    "Failed to extract recovered buffers for checkpoint " + barrier.getId(),
                    CheckpointFailureReason.CHECKPOINT_DECLINED,
                    e);
        }
    }

    public void checkpointStopped(long checkpointId) {
        channelStatePersister.stopPersisting(checkpointId);
    }

    @Override
    protected void requestSubpartitions() throws IOException {
        checkState(toBeConsumedBuffers.isEmpty());

        boolean retriggerRequest = false;
        boolean notifyDataAvailable = false;

        // The lock is required to request only once in the presence of retriggered requests.
        synchronized (requestLock) {
            checkState(!isReleased, "LocalInputChannel has been released already");

            if (subpartitionView == null) {
                LOG.debug(
                        "{}: Requesting LOCAL subpartitions {} of partition {}. {}",
                        this,
                        consumedSubpartitionIndexSet,
                        partitionId,
                        channelStatePersister);

                try {
                    ResultSubpartitionView subpartitionView =
                            partitionManager.createSubpartitionView(
                                    partitionId, consumedSubpartitionIndexSet, this);

                    if (subpartitionView == null) {
                        throw new IOException("Error requesting subpartition.");
                    }

                    // make the subpartition view visible
                    this.subpartitionView = subpartitionView;

                    // check if the channel was released in the meantime
                    if (isReleased) {
                        subpartitionView.releaseAllResources();
                        this.subpartitionView = null;
                    } else {
                        notifyDataAvailable = true;
                        // Unblock any recovered-buffer producer awaiting the upstream view; from
                        // here on, the consumer can drain recoveredQueue without racing a null
                        // subpartitionView in checkAndWaitForSubpartitionView.
                        upstreamReady.complete(null);
                    }
                } catch (PartitionNotFoundException notFound) {
                    if (increaseBackoff()) {
                        retriggerRequest = true;
                    } else {
                        throw notFound;
                    }
                }
            }
        }

        if (notifyDataAvailable) {
            notifyDataAvailable(this.subpartitionView);
        }

        // Do this outside of the lock scope as this might lead to a
        // deadlock with a concurrent release of the channel via the
        // input gate.
        if (retriggerRequest) {
            inputGate.retriggerPartitionRequest(partitionId.getPartitionId(), channelInfo);
        }
    }

    /** Retriggers a subpartition request. */
    void retriggerSubpartitionRequest(Timer timer) {
        synchronized (requestLock) {
            checkState(subpartitionView == null, "already requested partition");

            timer.schedule(
                    new TimerTask() {
                        @Override
                        public void run() {
                            try {
                                requestSubpartitions();
                            } catch (Throwable t) {
                                setError(t);
                            }
                        }
                    },
                    getCurrentBackoff());
        }
    }

    @Override
    protected int peekNextBufferSubpartitionIdInternal() throws IOException {
        checkError();

        ResultSubpartitionView subpartitionView = this.subpartitionView;
        if (subpartitionView == null) {
            // There is a possible race condition between writing a EndOfPartitionEvent (1) and
            // flushing (3) the Local
            // channel on the sender side, and reading EndOfPartitionEvent (2) and processing flush
            // notification (4). When
            // they happen in that order (1 - 2 - 3 - 4), flush notification can re-enqueue
            // LocalInputChannel after (or
            // during) it was released during reading the EndOfPartitionEvent (2).
            if (isReleased) {
                return -1;
            }

            // this can happen if the request for the partition was triggered asynchronously
            // by the time trigger
            // would be good to avoid that, by guaranteeing that the requestPartition() and
            // getNextBuffer() always come from the same thread
            // we could do that by letting the timer insert a special "requesting channel" into the
            // input gate's queue
            subpartitionView = checkAndWaitForSubpartitionView();
        }

        return subpartitionView.peekNextBufferSubpartitionId();
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer() throws IOException {
        checkError();

        boolean inRecovery;
        synchronized (recoveredQueue) {
            inRecovery = recoveredQueue.isInRecovery();
        }

        if (inRecovery) {
            if (hasPendingPriorityEvent) {
                return pullPriorityFromSubpartitionView();
            }
            Buffer buf;
            synchronized (recoveredQueue) {
                if (recoveredQueue.isEmpty()) {
                    // Drain not finished yet; block normal upstream data until delivery completes.
                    return Optional.empty();
                }
                buf = recoveredQueue.poll();
            }
            return wrapRecoveredBufferAsAvailability(buf);
        }

        if (!toBeConsumedBuffers.isEmpty()) {
            return getBufferAndAvailability(toBeConsumedBuffers.removeFirst());
        }

        ResultSubpartitionView subpartitionView = this.subpartitionView;
        if (subpartitionView == null) {
            // There is a possible race condition between writing a EndOfPartitionEvent (1) and
            // flushing (3) the Local
            // channel on the sender side, and reading EndOfPartitionEvent (2) and processing flush
            // notification (4). When
            // they happen in that order (1 - 2 - 3 - 4), flush notification can re-enqueue
            // LocalInputChannel after (or
            // during) it was released during reading the EndOfPartitionEvent (2).
            if (isReleased) {
                return Optional.empty();
            }

            // this can happen if the request for the partition was triggered asynchronously
            // by the time trigger
            // would be good to avoid that, by guaranteeing that the requestPartition() and
            // getNextBuffer() always come from the same thread
            // we could do that by letting the timer insert a special "requesting channel" into the
            // input gate's queue
            subpartitionView = checkAndWaitForSubpartitionView();
        }

        BufferAndBacklog next = subpartitionView.getNextBuffer();
        // ignore the empty buffer directly
        while (next != null && next.buffer().readableBytes() == 0) {
            next.buffer().recycleBuffer();
            next = subpartitionView.getNextBuffer();
            numBuffersIn.inc();
        }

        if (next == null) {
            if (subpartitionView.isReleased()) {
                throw new CancelTaskException(
                        "Consumed partition " + subpartitionView + " has been released.");
            } else {
                return Optional.empty();
            }
        }

        Buffer buffer = next.buffer();

        if (buffer instanceof FullyFilledBuffer) {
            List<Buffer> partialBuffers = ((FullyFilledBuffer) buffer).getPartialBuffers();
            int seq = next.getSequenceNumber();
            for (Buffer partialBuffer : partialBuffers) {
                toBeConsumedBuffers.add(
                        new BufferAndBacklog(
                                partialBuffer,
                                next.buffersInBacklog(),
                                buffer.getDataType(),
                                seq++));
            }

            return getBufferAndAvailability(toBeConsumedBuffers.removeFirst());
        }

        return getBufferAndAvailability(next);
    }

    /**
     * Pulls the pending priority event from {@code subpartitionView}, resets {@code
     * hasPendingPriorityEvent} if no further priority events follow, and selects the final {@code
     * nextDataType} via {@link #peekNextDataType(Buffer.DataType)} when the priority chain ends.
     */
    private Optional<BufferAndAvailability> pullPriorityFromSubpartitionView() throws IOException {
        checkState(subpartitionView != null, "No subpartition view available");
        BufferAndBacklog next = subpartitionView.getNextBuffer();
        checkState(
                next != null && next.buffer().getDataType().hasPriority(),
                "Expected priority event, but got %s",
                next == null ? "null" : next.buffer().getDataType());

        channelStatePersister.checkForBarrier(next.buffer());

        Buffer.DataType expectedNextDataType = next.getNextDataType();
        if (!expectedNextDataType.hasPriority()) {
            // Priority chain ended; the next buffer may come from recoveredQueue instead of the
            // view, so let peekNextDataType decide whose head wins.
            hasPendingPriorityEvent = false;
            expectedNextDataType = peekNextDataType(next.getNextDataType());
        }

        return Optional.of(
                new BufferAndAvailability(
                        next.buffer(),
                        expectedNextDataType,
                        next.buffersInBacklog(),
                        next.getSequenceNumber()));
    }

    /**
     * Wraps a raw {@code Buffer} polled from {@code recoveredQueue} into a {@link
     * BufferAndAvailability}, computing {@code nextDataType} via {@link
     * #peekNextDataType(Buffer.DataType)} with a lossy {@code subpartitionView} probe as the
     * upstream fallback.
     */
    private Optional<BufferAndAvailability> wrapRecoveredBufferAsAvailability(Buffer buf)
            throws IOException {
        if (buf instanceof FileRegionBuffer) {
            buf = ((FileRegionBuffer) buf).readInto(inputGate.getUnpooledSegment());
        }
        if (buf instanceof CompositeBuffer) {
            buf = ((CompositeBuffer) buf).getFullBufferData(inputGate.getUnpooledSegment());
        }

        numBytesIn.inc(buf.readableBytes());
        numBuffersIn.inc();

        // Lossy probe: subpartitionView has no peek-data-type API, so we can only distinguish
        // DATA_BUFFER from NONE. peekNextDataType only consults this when the recovery queue is
        // drained AND the producer has finished.
        ResultSubpartitionView view = subpartitionView;
        Buffer.DataType upstreamProbe;
        if (view != null && view.getAvailabilityAndBacklog(true).isAvailable()) {
            upstreamProbe = Buffer.DataType.DATA_BUFFER;
        } else {
            upstreamProbe = Buffer.DataType.NONE;
        }

        int sequenceNumber;
        synchronized (recoveredQueue) {
            Buffer.DataType nextDataType = peekNextDataType(upstreamProbe);
            sequenceNumber = recoveredQueue.nextSequenceNumber();
            NetworkActionsLogger.traceInput(
                    "LocalInputChannel#getNextBuffer",
                    buf,
                    inputGate.getOwningTaskName(),
                    channelInfo,
                    channelStatePersister,
                    sequenceNumber);
            return Optional.of(new BufferAndAvailability(buf, nextDataType, 0, sequenceNumber));
        }
    }

    /**
     * Returns the {@code DataType} the next {@link #getNextBuffer()} call will produce.
     *
     * <p>Decision order:
     *
     * <ol>
     *   <li>Recovery queue non-empty → head's {@code DataType}.
     *   <li>Drain producer not finished yet → {@code NONE} (subpartitionView may hold live data
     *       that must not be exposed during recovery).
     *   <li>Otherwise → {@code nextDataTypeOnUpstream}, supplied by the caller.
     * </ol>
     */
    private Buffer.DataType peekNextDataType(Buffer.DataType nextDataTypeOnUpstream) {
        synchronized (recoveredQueue) {
            if (!recoveredQueue.isEmpty()) {
                return recoveredQueue.peek().getDataType();
            }
            if (!recoveredQueue.isAllDelivered()) {
                return Buffer.DataType.NONE;
            }
        }
        return nextDataTypeOnUpstream;
    }

    private Optional<BufferAndAvailability> getBufferAndAvailability(BufferAndBacklog next)
            throws IOException {
        Buffer buffer = next.buffer();
        if (buffer instanceof FileRegionBuffer) {
            buffer = ((FileRegionBuffer) buffer).readInto(inputGate.getUnpooledSegment());
        }

        if (buffer instanceof CompositeBuffer) {
            buffer = ((CompositeBuffer) buffer).getFullBufferData(inputGate.getUnpooledSegment());
        }

        numBytesIn.inc(buffer.readableBytes());
        numBuffersIn.inc();
        channelStatePersister.checkForBarrier(buffer);
        channelStatePersister.maybePersist(buffer);
        NetworkActionsLogger.traceInput(
                "LocalInputChannel#getNextBuffer",
                buffer,
                inputGate.getOwningTaskName(),
                channelInfo,
                channelStatePersister,
                next.getSequenceNumber());
        return Optional.of(
                new BufferAndAvailability(
                        buffer,
                        next.getNextDataType(),
                        next.buffersInBacklog(),
                        next.getSequenceNumber()));
    }

    @Override
    public void notifyDataAvailable(ResultSubpartitionView view) {
        notifyChannelNonEmpty();
    }

    @Override
    public void notifyPriorityEvent(int prioritySequenceNumber) {
        // Force getNextBuffer() to pull from subpartitionView before continuing recoveredQueue.
        hasPendingPriorityEvent = true;
        super.notifyPriorityEvent(prioritySequenceNumber);
    }

    private ResultSubpartitionView checkAndWaitForSubpartitionView() {
        // synchronizing on the request lock means this blocks until the asynchronous request
        // for the partition view has been completed
        // by then the subpartition view is visible or the channel is released
        synchronized (requestLock) {
            checkState(!isReleased, "released");
            checkState(
                    subpartitionView != null,
                    "Queried for a buffer before requesting the subpartition.");
            return subpartitionView;
        }
    }

    @Override
    public void resumeConsumption() {
        checkState(!isReleased, "Channel released.");

        ResultSubpartitionView subpartitionView = checkNotNull(this.subpartitionView);
        subpartitionView.resumeConsumption();

        if (subpartitionView.getAvailabilityAndBacklog(true).isAvailable()) {
            notifyChannelNonEmpty();
        }
    }

    @Override
    public void acknowledgeAllRecordsProcessed() throws IOException {
        checkState(!isReleased, "Channel released.");

        subpartitionView.acknowledgeAllDataProcessed();
    }

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    @Override
    void sendTaskEvent(TaskEvent event) throws IOException {
        checkError();
        checkState(
                subpartitionView != null,
                "Tried to send task event to producer before requesting the subpartition.");

        if (!taskEventPublisher.publish(partitionId, event)) {
            throw new IOException(
                    "Error while publishing event "
                            + event
                            + " to producer. The producer could not be found.");
        }
    }

    // ------------------------------------------------------------------------
    // Life cycle
    // ------------------------------------------------------------------------

    @Override
    boolean isReleased() {
        return isReleased;
    }

    /** Releases the partition reader. */
    @Override
    void releaseAllResources() throws IOException {
        if (!isReleased) {
            isReleased = true;

            // Unblock any thread awaiting upstreamReady so it does not deadlock during shutdown;
            // the awaiter then recycles its buffer because isReleased is now true.
            upstreamReady.completeExceptionally(new CancelTaskException("Channel released."));

            ResultSubpartitionView view = subpartitionView;
            if (view != null) {
                view.releaseAllResources();
                subpartitionView = null;
            }

            synchronized (recoveredQueue) {
                recoveredQueue.releaseAll();
            }
            for (BufferAndBacklog bufferAndBacklog : toBeConsumedBuffers) {
                bufferAndBacklog.buffer().recycleBuffer();
            }
            toBeConsumedBuffers.clear();
        }
    }

    @Override
    void announceBufferSize(int newBufferSize) {
        checkState(!isReleased, "Channel released.");

        ResultSubpartitionView view = this.subpartitionView;
        if (view != null) {
            view.notifyNewBufferSize(newBufferSize);
        }
    }

    @Override
    int getBuffersInUseCount() {
        ResultSubpartitionView view = this.subpartitionView;
        return recoveredQueue.size()
                + toBeConsumedBuffers.size()
                + (view == null ? 0 : view.getNumberOfQueuedBuffers());
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        ResultSubpartitionView view = subpartitionView;

        int count = recoveredQueue.size() + toBeConsumedBuffers.size();
        if (view != null) {
            count += view.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return count;
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        if (subpartitionView != null) {
            checkNotNull(subpartitionView).notifyRequiredSegmentId(subpartitionId, segmentId);
        }
    }

    @Override
    public String toString() {
        return "LocalInputChannel [" + partitionId + "]";
    }

    // ------------------------------------------------------------------------
    // Getter
    // ------------------------------------------------------------------------

    @VisibleForTesting
    ResultSubpartitionView getSubpartitionView() {
        return subpartitionView;
    }
}
