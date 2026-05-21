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
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Helpers that wire a {@link SpillFileReader} onto the existing {@code channelIOExecutor} once
 * conversion has run on the task thread. Keeps the channel-collection and reader construction
 * details out of the task class itself.
 */
@Internal
public final class SpillFileReaderBootstrap {

    private SpillFileReaderBootstrap() {}

    /**
     * Collects all {@link RecoveredInputChannel}s currently attached to the given input gates,
     * keyed by {@link InputChannelInfo}. Returns an insertion-ordered map. Called by the task
     * thread before {@code requestPartitions()} converts the recovered channels to their physical
     * forms — the resulting map is the source-channel set for {@link
     * RecoveredChannelBufferRequester}.
     */
    public static Map<InputChannelInfo, RecoveredInputChannel> collectRecoveredChannels(
            InputGate[] inputGates) {
        Map<InputChannelInfo, RecoveredInputChannel> map = new LinkedHashMap<>();
        for (InputGate gate : inputGates) {
            int n = gate.getNumberOfInputChannels();
            for (int i = 0; i < n; i++) {
                InputChannel ch = gate.getChannel(i);
                if (ch instanceof RecoveredInputChannel) {
                    map.put(ch.getChannelInfo(), (RecoveredInputChannel) ch);
                }
            }
        }
        return map;
    }

    /**
     * Collects all physical {@link RecoverableInputChannel}s currently attached to the given input
     * gates. Called by the task thread after {@code requestPartitions()} has run conversion, at
     * which point every channel implements the recoverable interface (the physical Local/Remote
     * channels) and the buffers delivered by the drain land in their queues.
     */
    public static List<RecoverableInputChannel> collectPhysicalChannels(InputGate[] inputGates) {
        java.util.ArrayList<RecoverableInputChannel> list = new java.util.ArrayList<>();
        for (InputGate gate : inputGates) {
            int n = gate.getNumberOfInputChannels();
            for (int i = 0; i < n; i++) {
                InputChannel ch = gate.getChannel(i);
                if (ch instanceof RecoverableInputChannel) {
                    list.add((RecoverableInputChannel) ch);
                }
            }
        }
        return list;
    }

    /**
     * Constructs a {@link SpillFileReader} from a frozen {@link SpillFile} and the
     * before/after-conversion channel sets. Used by the task thread after conversion. The {@code
     * InputChannelInfo}-keyed map is derived internally from {@code physicalChannels}.
     */
    public static SpillFileReader buildReader(
            SpillFile spillFile,
            Map<InputChannelInfo, RecoveredInputChannel> sourceChannels,
            List<RecoverableInputChannel> physicalChannels) {
        return new SpillFileReader(
                spillFile,
                physicalChannels,
                new RecoveredChannelBufferRequester(new HashMap<>(sourceChannels)));
    }
}
