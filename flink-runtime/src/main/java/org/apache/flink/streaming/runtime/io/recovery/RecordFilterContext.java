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

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Context containing all information needed for filtering recovered channel state buffers.
 *
 * <p>This context encapsulates the input configurations, rescaling descriptor, and subtask
 * information required by the channel-state-unspilling thread to perform record filtering during
 * recovery.
 *
 * <p>Supports multiple inputs (e.g., TwoInputStreamTask, MultipleInputStreamTask) by storing a list
 * of {@link InputFilterConfig} instances indexed by input index.
 *
 * <p>Use the constructor with empty inputConfigs or enabled=false when filtering is not needed.
 */
@Internal
public class RecordFilterContext {

    /** Configuration for filtering records on a specific input. */
    public static class InputFilterConfig {
        private final TypeSerializer<?> typeSerializer;
        private final StreamPartitioner<?> partitioner;
        private final int numberOfChannels;

        /**
         * Creates a new InputFilterConfig.
         *
         * @param typeSerializer Serializer for the record type.
         * @param partitioner Partitioner used to determine record ownership.
         * @param numberOfChannels The parallelism of this input.
         */
        public InputFilterConfig(
                TypeSerializer<?> typeSerializer,
                StreamPartitioner<?> partitioner,
                int numberOfChannels) {
            this.typeSerializer = checkNotNull(typeSerializer);
            this.partitioner = checkNotNull(partitioner);
            this.numberOfChannels = numberOfChannels;
        }

        public TypeSerializer<?> getTypeSerializer() {
            return typeSerializer;
        }

        public StreamPartitioner<?> getPartitioner() {
            return partitioner;
        }

        public int getNumberOfChannels() {
            return numberOfChannels;
        }
    }

    /** Input configurations indexed by input index. Can be empty but never null. */
    private final List<InputFilterConfig> inputConfigs;

    /** Descriptor containing rescaling information. Never null. */
    private final InflightDataRescalingDescriptor rescalingDescriptor;

    /** Current subtask index. */
    private final int subtaskIndex;

    /** Maximum parallelism for configuring partitioners. */
    private final int maxParallelism;

    /** Temporary directories for spilling spanning records. Can be empty but never null. */
    private final String[] tmpDirectories;

    /** Whether unaligned checkpoint during recovery is enabled. */
    private final boolean unalignedDuringRecoveryEnabled;

    /**
     * Creates a new RecordFilterContext.
     *
     * @param inputConfigs Input configurations indexed by input index. Can be empty but not null.
     * @param rescalingDescriptor Descriptor containing rescaling information. Not null.
     * @param subtaskIndex Current subtask index.
     * @param maxParallelism Maximum parallelism.
     * @param tmpDirectories Temporary directories for spilling spanning records. Can be null
     *     (converted to empty array).
     * @param unalignedDuringRecoveryEnabled Whether unaligned checkpoint during recovery is
     *     enabled.
     */
    public RecordFilterContext(
            List<InputFilterConfig> inputConfigs,
            InflightDataRescalingDescriptor rescalingDescriptor,
            int subtaskIndex,
            int maxParallelism,
            String[] tmpDirectories,
            boolean unalignedDuringRecoveryEnabled) {
        this.inputConfigs =
                Collections.unmodifiableList(new ArrayList<>(checkNotNull(inputConfigs)));
        this.rescalingDescriptor = checkNotNull(rescalingDescriptor);
        this.subtaskIndex = subtaskIndex;
        this.maxParallelism = maxParallelism;
        this.tmpDirectories = tmpDirectories != null ? tmpDirectories : new String[0];
        this.unalignedDuringRecoveryEnabled = unalignedDuringRecoveryEnabled;
    }

    /**
     * Gets the input configuration for a specific input.
     *
     * @param inputIndex The input index (0-based).
     * @return The input configuration for the specified input.
     * @throws IllegalArgumentException if inputIndex is out of bounds.
     */
    public InputFilterConfig getInputConfig(int inputIndex) {
        checkArgument(
                inputIndex >= 0 && inputIndex < inputConfigs.size(),
                "Invalid input index: %s, number of inputs: %s",
                inputIndex,
                inputConfigs.size());
        return inputConfigs.get(inputIndex);
    }

    /**
     * Gets the number of inputs.
     *
     * @return The number of inputs.
     */
    public int getNumberOfInputs() {
        return inputConfigs.size();
    }

    /**
     * Checks whether unaligned checkpoint during recovery is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise.
     */
    public boolean isUnalignedDuringRecoveryEnabled() {
        return unalignedDuringRecoveryEnabled;
    }

    /**
     * Gets the rescaling descriptor.
     *
     * @return The descriptor containing rescaling information.
     */
    public InflightDataRescalingDescriptor getRescalingDescriptor() {
        return rescalingDescriptor;
    }

    /**
     * Gets the current subtask index.
     *
     * @return The subtask index.
     */
    public int getSubtaskIndex() {
        return subtaskIndex;
    }

    /**
     * Gets the maximum parallelism.
     *
     * @return The maximum parallelism value.
     */
    public int getMaxParallelism() {
        return maxParallelism;
    }

    /**
     * Gets the temporary directories for spilling spanning records.
     *
     * @return The temporary directories, never null (may be empty array).
     */
    public String[] getTmpDirectories() {
        return tmpDirectories;
    }

    /**
     * Checks if a specific gate and subtask combination is ambiguous (requires filtering).
     *
     * @param gateIndex The gate index.
     * @param oldSubtaskIndex The old subtask index.
     * @return true if enabled and the channel is ambiguous and records need filtering.
     */
    public boolean isAmbiguous(int gateIndex, int oldSubtaskIndex) {
        return unalignedDuringRecoveryEnabled
                && rescalingDescriptor.isAmbiguous(gateIndex, oldSubtaskIndex);
    }

    /**
     * Creates a disabled RecordFilterContext for testing or when filtering is not needed.
     *
     * <p>The returned context has empty inputConfigs and enabled=false, so needsFiltering() will
     * always return false.
     *
     * @return A disabled RecordFilterContext.
     */
    public static RecordFilterContext disabled() {
        return new RecordFilterContext(
                Collections.emptyList(),
                InflightDataRescalingDescriptor.NO_RESCALE,
                0,
                0,
                new String[0],
                false);
    }
}
