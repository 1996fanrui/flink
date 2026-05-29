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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Helpers that collect input channels and construct a {@link SpillFileDrainer}. */
@Internal
public final class SpillFileReaderBootstrap {

    private SpillFileReaderBootstrap() {}

    /**
     * Collects post-conversion {@link RecoverableInputChannel}s (the physical Local/Remote
     * channels) from the gates; their queues receive drain deliveries.
     */
    public static List<RecoverableInputChannel> collectPhysicalChannels(InputGate[] inputGates) {
        ArrayList<RecoverableInputChannel> list = new ArrayList<>();
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
     * Constructs a {@link SpillFileDrainer} from a frozen {@link SpillFile} and a future that will
     * be completed with the post-conversion physical channel set. Drain buffer allocation is
     * delegated to each physical channel via {@code requestRecoveryBufferBlocking()}.
     */
    public static SpillFileDrainer buildDrainer(
            SpillFile spillFile,
            CompletableFuture<List<RecoverableInputChannel>> physicalChannelsFuture) {
        return new SpillFileDrainer(spillFile, physicalChannelsFuture);
    }
}
