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

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Filters recovered channel-state buffers during rescaling. */
@Internal
public class ChannelStateFilteringHandler implements Closeable {

    // Wildcard allows heterogeneous record types across gates.
    private final GateFilterHandler<?>[] gateHandlers;

    ChannelStateFilteringHandler(GateFilterHandler<?>[] gateHandlers) {
        this.gateHandlers = checkNotNull(gateHandlers);
    }

    /** Returns {@code null} when no gate needs filtering. */
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
     * Filters one recovered buffer and writes surviving records to {@code bufferSupplier}. The
     * deserializer may retain partial records across calls.
     */
    public void filterAndRewrite(
            int gateIndex,
            int oldSubtaskIndex,
            int oldChannelIndex,
            InputChannelInfo newChannelInfo,
            Buffer sourceBuffer,
            BufferSupplier bufferSupplier)
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
                oldSubtaskIndex, oldChannelIndex, newChannelInfo, sourceBuffer, bufferSupplier);
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
        if (tmpDirectories != null && tmpDirectories.length > 0) {
            return new SpillingAdaptiveSpanningRecordDeserializer<>(tmpDirectories);
        } else {
            String[] defaultDirs = new String[] {System.getProperty("java.io.tmpdir")};
            return new SpillingAdaptiveSpanningRecordDeserializer<>(defaultDirs);
        }
    }

    // -------------------------------------------------------------------------------------------
    // Inner classes
    // -------------------------------------------------------------------------------------------

    /**
     * Provides buffers for re-serializing filtered records, tagged with the destination channel.
     */
    @FunctionalInterface
    public interface BufferSupplier {
        Buffer requestBufferBlocking(InputChannelInfo channelInfo)
                throws IOException, InterruptedException;
    }

    static class GateFilterHandler<T> {

        private final Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels;
        private final StreamElementSerializer<T> serializer;
        private final DeserializationDelegate<StreamElement> deserializationDelegate;
        private final DataOutputSerializer outputSerializer;
        private final byte[] lengthBuffer = new byte[4];

        GateFilterHandler(
                Map<SubtaskConnectionDescriptor, VirtualChannel<T>> virtualChannels,
                StreamElementSerializer<T> serializer) {
            this.virtualChannels = checkNotNull(virtualChannels);
            this.serializer = checkNotNull(serializer);
            this.deserializationDelegate = new NonReusingDeserializationDelegate<>(serializer);
            this.outputSerializer = new DataOutputSerializer(128);
        }

        void filterAndRewrite(
                int oldSubtaskIndex,
                int oldChannelIndex,
                InputChannelInfo newChannelInfo,
                Buffer sourceBuffer,
                BufferSupplier bufferSupplier)
                throws IOException, InterruptedException {

            boolean sourceBufferOwnershipTransferred = false;
            Buffer currentBuffer = null;
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
                        if (currentBuffer == null) {
                            currentBuffer = bufferSupplier.requestBufferBlocking(newChannelInfo);
                        }
                        currentBuffer =
                                serializeElement(
                                        deserializationDelegate.getInstance(),
                                        newChannelInfo,
                                        currentBuffer,
                                        bufferSupplier);
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

        private Buffer serializeElement(
                StreamElement element,
                InputChannelInfo newChannelInfo,
                Buffer currentBuffer,
                BufferSupplier bufferSupplier)
                throws IOException, InterruptedException {
            outputSerializer.clear();
            serializer.serialize(element, outputSerializer);
            int recordLength = outputSerializer.length();

            writeLengthToBuffer(recordLength);
            currentBuffer =
                    writeDataToBuffer(
                            lengthBuffer, 0, 4, newChannelInfo, currentBuffer, bufferSupplier);

            byte[] serializedData = outputSerializer.getSharedBuffer();
            currentBuffer =
                    writeDataToBuffer(
                            serializedData,
                            0,
                            recordLength,
                            newChannelInfo,
                            currentBuffer,
                            bufferSupplier);
            return currentBuffer;
        }

        private void writeLengthToBuffer(int length) {
            lengthBuffer[0] = (byte) (length >> 24);
            lengthBuffer[1] = (byte) (length >> 16);
            lengthBuffer[2] = (byte) (length >> 8);
            lengthBuffer[3] = (byte) length;
        }

        private Buffer writeDataToBuffer(
                byte[] data,
                int dataOffset,
                int dataLength,
                InputChannelInfo newChannelInfo,
                Buffer currentBuffer,
                BufferSupplier bufferSupplier)
                throws IOException, InterruptedException {
            int offset = dataOffset;
            int remaining = dataLength;

            while (remaining > 0) {
                int writableBytes = currentBuffer.getMaxCapacity() - currentBuffer.getSize();

                if (writableBytes == 0) {
                    currentBuffer = bufferSupplier.requestBufferBlocking(newChannelInfo);
                    writableBytes = currentBuffer.getMaxCapacity();
                }

                int bytesToWrite = Math.min(remaining, writableBytes);
                currentBuffer
                        .getMemorySegment()
                        .put(
                                currentBuffer.getMemorySegmentOffset() + currentBuffer.getSize(),
                                data,
                                offset,
                                bytesToWrite);
                currentBuffer.setSize(currentBuffer.getSize() + bytesToWrite);

                offset += bytesToWrite;
                remaining -= bytesToWrite;
            }
            return currentBuffer;
        }

        boolean hasPartialData() {
            return virtualChannels.values().stream().anyMatch(VirtualChannel::hasPartialData);
        }

        void clear() {
            virtualChannels.values().forEach(VirtualChannel::clear);
        }
    }
}
