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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Helper class for persisting channel state via {@link ChannelStateWriter}. */
@NotThreadSafe
public final class ChannelStatePersister {
    private static final Logger LOG = LoggerFactory.getLogger(ChannelStatePersister.class);

    private final InputChannelInfo channelInfo;

    private enum CheckpointStatus {
        COMPLETED,
        BARRIER_PENDING,
        BARRIER_RECEIVED
    }

    private CheckpointStatus checkpointStatus = CheckpointStatus.COMPLETED;

    private long lastSeenBarrier = -1L;

    /**
     * Writer must be initialized before usage. {@link #startPersisting(long, RecoveredBufferStore,
     * List)} enforces this invariant.
     */
    private final ChannelStateWriter channelStateWriter;

    ChannelStatePersister(ChannelStateWriter channelStateWriter, InputChannelInfo channelInfo) {
        this.channelStateWriter = checkNotNull(channelStateWriter);
        this.channelInfo = checkNotNull(channelInfo);
    }

    /**
     * Starts persisting channel state for the given checkpoint barrier.
     *
     * <p>Performs the following steps in order:
     *
     * <ol>
     *   <li>Validates that this checkpoint has not been superseded (throws {@link
     *       CheckpointException} if a newer barrier was already received).
     *   <li>Asserts the disk-network exclusivity invariant: {@code store.isEmpty() || knownBuffers.isEmpty()}.
     *       Guaranteed by {@code UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true} (upstream does not
     *       replay output state) combined with {@link RemoteInputChannel#getNextBuffer} draining the
     *       store before polling {@code receivedBuffers}.
     *   <li>Delegates ready-buffer snapshot and FilteredBufferDispatcher callback to {@link
     *       RecoveredBufferStore#checkpoint}.
     *   <li>Writes network inflight buffers (Remote only) via {@link
     *       ChannelStateWriter#addInputData}.
     * </ol>
     *
     * @param barrierId the barrier/checkpoint ID
     * @param store the per-channel recovered buffer store (use {@link RecoveredBufferStore#EMPTY}
     *     when no recovery data is present)
     * @param knownBuffers network inflight buffers to persist (empty for LocalInputChannel)
     * @throws CheckpointException if checkpointing fails or the checkpoint has been superseded
     */
    protected void startPersisting(
            long barrierId, RecoveredBufferStore store, List<Buffer> knownBuffers)
            throws CheckpointException {
        logEvent("startPersisting", barrierId);
        if (checkpointStatus == CheckpointStatus.BARRIER_RECEIVED && lastSeenBarrier > barrierId) {
            throw new CheckpointException(
                    String.format(
                            "Barrier for newer checkpoint %d has already been received compared to the requested checkpoint %d",
                            lastSeenBarrier, barrierId),
                    CheckpointFailureReason
                            .CHECKPOINT_SUBSUMED); // currently, at most one active unaligned
        }
        if (lastSeenBarrier < barrierId) {
            // Regardless of the current checkpointStatus, if we are notified about a more recent
            // checkpoint then we have seen so far, always mark that this more recent barrier is
            // pending.
            // BARRIER_RECEIVED status can happen if we have seen an older barrier, that probably
            // has not yet been processed by the task, but task is now notifying us that checkpoint
            // has started for even newer checkpoint. We should spill the knownBuffers and mark that
            // we are waiting for that newer barrier to arrive
            checkpointStatus = CheckpointStatus.BARRIER_PENDING;
            lastSeenBarrier = barrierId;
        }

        // Defensive invariant check: store non-empty and knownBuffers non-empty must not coexist.
        // Guaranteed by UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true (upstream has no output state
        // to replay, so receivedBuffers stays empty of DATA_BUFFER while the recovered store is
        // draining) together with RemoteInputChannel#getNextBuffer draining the store first.
        // Violations here indicate one of those assumptions broke and must fail-fast rather than
        // silently produce corrupt channel state.
        checkState(
                store.isEmpty() || knownBuffers.isEmpty(),
                "Invariant violated: store has data (size=%s) AND knownBuffers non-empty (size=%s) at barrier %s. "
                        + "Requires UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true so upstream does not "
                        + "replay output state into receivedBuffers while the recovered store is still draining.",
                store.size(),
                knownBuffers.size(),
                barrierId);

        // Step 1: snapshot ready buffers and fire FilteredBufferDispatcher callback via
        // store.checkpoint().
        try {
            store.checkpoint(channelStateWriter, barrierId, channelInfo);
        } catch (IOException e) {
            throw new CheckpointException(
                    "Failed to checkpoint recovered store",
                    CheckpointFailureReason.IO_EXCEPTION,
                    e);
        }

        // Step 2: persist network inflight buffers (Remote only; Local always passes emptyList).
        if (!knownBuffers.isEmpty()) {
            channelStateWriter.addInputData(
                    barrierId,
                    channelInfo,
                    ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                    CloseableIterator.fromList(knownBuffers, Buffer::recycleBuffer));
        }
    }

    protected void stopPersisting(long id) {
        logEvent("stopPersisting", id);
        if (id >= lastSeenBarrier) {
            checkpointStatus = CheckpointStatus.COMPLETED;
            lastSeenBarrier = id;
        }
    }

    protected void maybePersist(Buffer buffer) {
        if (checkpointStatus == CheckpointStatus.BARRIER_PENDING && buffer.isBuffer()) {
            channelStateWriter.addInputData(
                    lastSeenBarrier,
                    channelInfo,
                    ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                    CloseableIterator.ofElement(buffer.retainBuffer(), Buffer::recycleBuffer));
        }
    }

    protected OptionalLong checkForBarrier(Buffer buffer) throws IOException {
        AbstractEvent event = parseEvent(buffer);
        if (event instanceof CheckpointBarrier) {
            long barrierId = ((CheckpointBarrier) event).getId();
            long expectedBarrierId =
                    checkpointStatus == CheckpointStatus.COMPLETED
                            ? lastSeenBarrier + 1
                            : lastSeenBarrier;
            if (barrierId >= expectedBarrierId) {
                logEvent("found barrier", barrierId);
                checkpointStatus = CheckpointStatus.BARRIER_RECEIVED;
                lastSeenBarrier = barrierId;
                return OptionalLong.of(lastSeenBarrier);
            } else {
                logEvent("ignoring barrier", barrierId);
            }
        }
        if (event instanceof EventAnnouncement) { // NOTE: only remote channels
            EventAnnouncement announcement = (EventAnnouncement) event;
            if (announcement.getAnnouncedEvent() instanceof CheckpointBarrier) {
                long barrierId = ((CheckpointBarrier) announcement.getAnnouncedEvent()).getId();
                logEvent("found announcement for barrier", barrierId);
                return OptionalLong.of(barrierId);
            }
        }
        return OptionalLong.empty();
    }

    private void logEvent(String event, long barrierId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "{} {}, lastSeenBarrier = {} ({}) @ {}",
                    event,
                    barrierId,
                    lastSeenBarrier,
                    checkpointStatus,
                    channelInfo);
        }
    }

    /**
     * Parses the buffer as an event and returns the {@link CheckpointBarrier} if the event is
     * indeed a barrier or returns null in all other cases.
     */
    @Nullable
    protected AbstractEvent parseEvent(Buffer buffer) throws IOException {
        if (buffer.isBuffer()) {
            return null;
        } else {
            AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
            // reset the buffer because it would be deserialized again in SingleInputGate while
            // getting next buffer.
            // we can further improve to avoid double deserialization in the future.
            buffer.setReaderIndex(0);
            return event;
        }
    }

    protected boolean hasBarrierReceived() {
        return checkpointStatus == CheckpointStatus.BARRIER_RECEIVED;
    }

    @Override
    public String toString() {
        return "ChannelStatePersister(lastSeenBarrier="
                + lastSeenBarrier
                + " ("
                + checkpointStatus
                + ")}";
    }
}
