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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilter;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilterContext;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannel;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannelRecordFilterFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Filters recovered channel state buffers during the channel-state-unspilling phase, removing
 * records that do not belong to the current subtask after rescaling.
 *
 * <p>Uses a per-gate architecture: each {@link InputGate} gets its own {@link GateFilterHandler}
 * with the correct serializer, so multi-input tasks (e.g., TwoInputStreamTask) correctly
 * deserialize different record types on different gates.
 */
@Internal
public class ChannelStateFilteringHandler implements Closeable {

    // Wildcard allows heterogeneous record types across gates.
    private final GateFilterHandler<?>[] gateHandlers;

    ChannelStateFilteringHandler(GateFilterHandler<?>[] gateHandlers) {
        this.gateHandlers = checkNotNull(gateHandlers);
    }

    /**
     * Creates a handler from the recovery context, building per-gate virtual channels based on
     * rescaling descriptors. Returns {@code null} if no filtering is needed (e.g., source tasks or
     * no rescaling).
     */
    @Nullable
    public static ChannelStateFilteringHandler createFromContext(
            RecordFilterContext filterContext, InputGate[] inputGates) {
        // Source tasks have no network inputs
        if (filterContext.getNumberOfGates() == 0) {
            return null;
        }

        InflightDataRescalingDescriptor rescalingDescriptor =
                filterContext.getRescalingDescriptor();

        GateFilterHandler<?>[] gateHandlers = new GateFilterHandler<?>[inputGates.length];
        boolean hasAnyVirtualChannels = false;

        for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
            gateHandlers[gateIndex] =
                    createGateHandler(filterContext, inputGates, rescalingDescriptor, gateIndex);
            if (gateHandlers[gateIndex] != null) {
                hasAnyVirtualChannels = true;
            }
        }

        if (!hasAnyVirtualChannels) {
            return null;
        }

        return new ChannelStateFilteringHandler(gateHandlers);
    }

    /**
     * Filters a recovered buffer from the specified virtual channel, writing only the records that
     * belong to the current subtask directly into the spill writer.
     *
     * <p>Surviving records are serialized and written to {@code writer} without any intermediate
     * network buffer. One call may emit 0..N records depending on the filter result and whether
     * spanning records from previous buffers complete in this buffer.
     */
    public void filterAndRewrite(
            int gateIndex,
            int oldSubtaskIndex,
            int oldChannelIndex,
            InputChannelInfo newChannelInfo,
            Buffer sourceBuffer,
            FetchedChannelStateWriter writer)
            throws IOException, InterruptedException {

        if (gateIndex < 0 || gateIndex >= gateHandlers.length) {
            throw new IllegalStateException(
                    "Invalid gateIndex: "
                            + gateIndex
                            + ", number of gates: "
                            + gateHandlers.length);
        }

        GateFilterHandler<?> gateHandler = gateHandlers[gateIndex];
        if (gateHandler == null) {
            throw new IllegalStateException(
                    "No handler for gateIndex "
                            + gateIndex
                            + ". This gate is not a network input and should not have recovered buffers.");
        }
        gateHandler.filterAndRewrite(
                oldSubtaskIndex, oldChannelIndex, newChannelInfo, sourceBuffer, writer);
    }

    /** Returns {@code true} if any virtual channel has a partial (spanning) record pending. */
    public boolean hasPartialData() {
        for (GateFilterHandler<?> handler : gateHandlers) {
            if (handler != null && handler.hasPartialData()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        for (GateFilterHandler<?> handler : gateHandlers) {
            if (handler != null) {
                handler.clear();
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Private static helper methods
    // -------------------------------------------------------------------------------------------

    /**
     * Creates a {@link GateFilterHandler} for a single gate. The method-level type parameter
     * ensures type safety within each gate while allowing different gates to have different types.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    private static <T> GateFilterHandler<T> createGateHandler(
            RecordFilterContext filterContext,
            InputGate[] inputGates,
            InflightDataRescalingDescriptor rescalingDescriptor,
            int gateIndex) {
        RecordFilterContext.InputFilterConfig inputConfig = filterContext.getInputConfig(gateIndex);
        if (inputConfig == null) {
            throw new IllegalStateException(
                    "No InputFilterConfig for gateIndex "
                            + gateIndex
                            + ". This indicates a bug in RecordFilterContext initialization.");
        }

        InputGate gate = inputGates[gateIndex];
        int[] oldSubtaskIndexes = rescalingDescriptor.getOldSubtaskIndexes(gateIndex);
        RescaleMappings channelMapping = rescalingDescriptor.getChannelMapping(gateIndex);

        TypeSerializer<T> typeSerializer = (TypeSerializer<T>) inputConfig.getTypeSerializer();
        StreamElementSerializer<T> elementSerializer =
                new StreamElementSerializer<>(typeSerializer);

        VirtualChannelRecordFilterFactory<T> filterFactory =
                VirtualChannelRecordFilterFactory.fromContext(filterContext, gateIndex);

        Map<SubtaskConnectionDescriptor, VirtualChannel<T>> gateVirtualChannels = new HashMap<>();

        for (int oldSubtaskIndex : oldSubtaskIndexes) {
            int numChannels = gate.getNumberOfInputChannels();
            int[] oldChannelIndexes = getOldChannelIndexes(channelMapping, numChannels);

            for (int oldChannelIndex : oldChannelIndexes) {
                SubtaskConnectionDescriptor key =
                        new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex);

                if (gateVirtualChannels.containsKey(key)) {
                    continue;
                }

                // Only ambiguous channels need actual filtering; non-ambiguous ones pass through
                boolean isAmbiguous = rescalingDescriptor.isAmbiguous(gateIndex, oldSubtaskIndex);

                RecordFilter<T> recordFilter =
                        isAmbiguous
                                ? filterFactory.createFilter()
                                : VirtualChannelRecordFilterFactory.createPassThroughFilter();

                RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer =
                        createDeserializer(filterContext.getTmpDirectories());

                VirtualChannel<T> vc = new VirtualChannel<>(deserializer, recordFilter);
                gateVirtualChannels.put(key, vc);
            }
        }

        if (gateVirtualChannels.isEmpty()) {
            return null;
        }

        return new GateFilterHandler<>(gateVirtualChannels, elementSerializer);
    }

    /**
     * Collects all old channel indexes that are mapped from any new channel index in this gate.
     * channelMapping is new-to-old, so we iterate new indexes and collect their old counterparts.
     */
    private static int[] getOldChannelIndexes(RescaleMappings channelMapping, int numChannels) {
        List<Integer> oldIndexes = new ArrayList<>();
        for (int newIndex = 0; newIndex < numChannels; newIndex++) {
            int[] mapped = channelMapping.getMappedIndexes(newIndex);
            for (int oldIndex : mapped) {
                if (!oldIndexes.contains(oldIndex)) {
                    oldIndexes.add(oldIndex);
                }
            }
        }
        return oldIndexes.stream().mapToInt(Integer::intValue).toArray();
    }

    private static RecordDeserializer<DeserializationDelegate<StreamElement>> createDeserializer(
            String[] tmpDirectories) {
        checkArgument(
                tmpDirectories != null && tmpDirectories.length > 0,
                "Spilling temporary directories must not be empty.");
        return new SpillingAdaptiveSpanningRecordDeserializer<>(tmpDirectories);
    }

    // -------------------------------------------------------------------------------------------
    // Inner classes
    // -------------------------------------------------------------------------------------------

    /**
     * Handles record filtering for a single input gate. Each gate has its own serializer and set of
     * virtual channels, allowing different gates to handle different record types independently.
     */
    static class GateFilterHandler<T> {

        private final Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels;
        private final StreamElementSerializer<T> serializer;
        private final DeserializationDelegate<StreamElement> deserializationDelegate;
        private final DataOutputSerializer outputSerializer;

        GateFilterHandler(
                Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels,
                StreamElementSerializer<T> serializer) {
            this.virtualChannels = checkNotNull(virtualChannels);
            this.serializer = checkNotNull(serializer);
            this.deserializationDelegate = new NonReusingDeserializationDelegate<>(serializer);
            this.outputSerializer = new DataOutputSerializer(128);
        }

        /**
         * Deserializes records from {@code sourceBuffer}, applies the virtual channel's record
         * filter, and immediately re-serializes each surviving record directly into the spill
         * writer. No intermediate network buffer is used.
         */
        void filterAndRewrite(
                int oldSubtaskIndex,
                int oldChannelIndex,
                InputChannelInfo newChannelInfo,
                Buffer sourceBuffer,
                FetchedChannelStateWriter writer)
                throws IOException, InterruptedException {

            boolean sourceBufferOwnershipTransferred = false;
            try {
                SubtaskConnectionDescriptor key =
                        new SubtaskConnectionDescriptor(oldSubtaskIndex, oldChannelIndex);
                VirtualChannel<T> vc = virtualChannels.get(key);
                if (vc == null) {
                    throw new IllegalStateException(
                            "No VirtualChannel found for key: "
                                    + key
                                    + "; known channels are "
                                    + virtualChannels.keySet());
                }

                vc.setNextBuffer(sourceBuffer);
                sourceBufferOwnershipTransferred = true;

                while (true) {
                    DeserializationResult result = vc.getNextRecord(deserializationDelegate);
                    if (result.isFullRecord()) {
                        serializeElement(
                                deserializationDelegate.getInstance(), newChannelInfo, writer);
                    }
                    if (result.isBufferConsumed()) {
                        break;
                    }
                }
            } catch (Throwable t) {
                if (!sourceBufferOwnershipTransferred) {
                    sourceBuffer.recycleBuffer();
                }
                throw t;
            }
        }

        /**
         * Serializes a single stream element in length-prefixed format (4B length + record bytes)
         * and writes it directly to the spill writer's output stream.
         */
        private void serializeElement(
                StreamElement element,
                InputChannelInfo newChannelInfo,
                FetchedChannelStateWriter writer)
                throws IOException {
            outputSerializer.clear();
            serializer.serialize(element, outputSerializer);
            int recordLength = outputSerializer.length();
            writer.writeRecord(newChannelInfo, outputSerializer.getSharedBuffer(), recordLength);
        }

        boolean hasPartialData() {
            return virtualChannels.values().stream().anyMatch(VirtualChannel::hasPartialData);
        }

        void clear() {
            virtualChannels.values().forEach(VirtualChannel::clear);
        }
    }
}
