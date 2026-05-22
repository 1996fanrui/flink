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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.util.TestBufferFactory;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteRecoveredInputChannel}. */
class RemoteRecoveredInputChannelTest {

    /**
     * Verifies that the migration path inside {@code RecoveredInputChannel.toInputChannel()} now
     * uses {@link RecoverableInputChannel#onRecoveredStateBuffer} followed by {@link
     * RecoverableInputChannel#finishReadRecoveredState()}: every remaining buffer is delivered in
     * order to the physical {@link RemoteInputChannel}, and the resulting channel can be consumed
     * via {@code getNextBuffer()}.
     */
    @Test
    void testToInputChannelUsesOnRecoveredStateBufferAndFinishReadRecoveredState()
            throws Exception {
        SingleInputGate inputGate =
                new SingleInputGateBuilder().setCheckpointingDuringRecoveryEnabled(true).build();
        RemoteRecoveredInputChannel recoveredChannel =
                InputChannelBuilder.newBuilder()
                        .setStateWriter(ChannelStateWriter.NO_OP)
                        .buildRemoteRecoveredChannel(inputGate);

        Buffer b1 = TestBufferFactory.createBuffer(13);
        Buffer b2 = TestBufferFactory.createBuffer(24);
        recoveredChannel.onRecoveredStateBuffer(b1);
        recoveredChannel.onRecoveredStateBuffer(b2);
        recoveredChannel.finishReadRecoveredState();

        InputChannel converted = recoveredChannel.toInputChannel();
        assertThat(converted).isInstanceOf(RemoteInputChannel.class);
        RemoteInputChannel remoteChannel = (RemoteInputChannel) converted;
        inputGate.setInputChannels(remoteChannel);

        // Both buffers were migrated through the push interface and remain consumable in order.
        Optional<InputChannel.BufferAndAvailability> first = remoteChannel.getNextBuffer();
        assertThat(first).isPresent();
        assertThat(first.get().buffer().getSize()).isEqualTo(13);

        Optional<InputChannel.BufferAndAvailability> second = remoteChannel.getNextBuffer();
        assertThat(second).isPresent();
        assertThat(second.get().buffer().getSize()).isEqualTo(24);

        // RecoveredInputChannel.finishReadRecoveredState appends an EndOfInputChannelStateEvent
        // sentinel into receivedBuffers, which is also migrated into the new channel's
        // recoveredBuffers queue. Drain it to fully complete the recovery phase.
        Optional<InputChannel.BufferAndAvailability> tail = remoteChannel.getNextBuffer();
        assertThat(tail).isPresent();
    }
}
