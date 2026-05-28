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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.NoOpResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateTest.TestingResultPartitionManager;

/** Builder for various {@link InputChannel} types. */
public class InputChannelBuilder {
    public static final ConnectionID STUB_CONNECTION_ID =
            new ConnectionID(ResourceID.generate(), new InetSocketAddress("localhost", 5000), 0);

    private int channelIndex = 0;
    private ResultPartitionID partitionId = new ResultPartitionID();
    private ResultSubpartitionIndexSet subpartitionIndexSet = new ResultSubpartitionIndexSet(0);
    private ConnectionID connectionID = STUB_CONNECTION_ID;
    private ResultPartitionManager partitionManager =
            new TestingResultPartitionManager(new NoOpResultSubpartitionView());
    private TaskEventPublisher taskEventPublisher = new TaskEventDispatcher();
    private ChannelStateWriter stateWriter = ChannelStateWriter.NO_OP;
    private ConnectionManager connectionManager = new TestingConnectionManager();
    private int initialBackoff = 0;
    private int maxBackoff = 0;
    private int partitionRequestListenerTimeout = 0;
    private int networkBuffersPerChannel = 2;
    private InputChannelMetrics metrics =
            InputChannelTestUtils.newUnregisteredInputChannelMetrics();

    public static InputChannelBuilder newBuilder() {
        return new InputChannelBuilder();
    }

    public InputChannelBuilder setChannelIndex(int channelIndex) {
        this.channelIndex = channelIndex;
        return this;
    }

    public InputChannelBuilder setPartitionId(ResultPartitionID partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public InputChannelBuilder setSubpartitionIndexSet(
            ResultSubpartitionIndexSet subpartitionIndexSet) {
        this.subpartitionIndexSet = subpartitionIndexSet;
        return this;
    }

    public InputChannelBuilder setPartitionManager(ResultPartitionManager partitionManager) {
        this.partitionManager = partitionManager;
        return this;
    }

    InputChannelBuilder setTaskEventPublisher(TaskEventPublisher taskEventPublisher) {
        this.taskEventPublisher = taskEventPublisher;
        return this;
    }

    public InputChannelBuilder setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
        return this;
    }

    public InputChannelBuilder setInitialBackoff(int initialBackoff) {
        this.initialBackoff = initialBackoff;
        return this;
    }

    public InputChannelBuilder setMaxBackoff(int maxBackoff) {
        this.maxBackoff = maxBackoff;
        return this;
    }

    public InputChannelBuilder setPartitionRequestListenerTimeout(
            int partitionRequestListenerTimeout) {
        this.partitionRequestListenerTimeout = partitionRequestListenerTimeout;
        return this;
    }

    public InputChannelBuilder setNetworkBuffersPerChannel(int networkBuffersPerChannel) {
        this.networkBuffersPerChannel = networkBuffersPerChannel;
        return this;
    }

    public InputChannelBuilder setMetrics(InputChannelMetrics metrics) {
        this.metrics = metrics;
        return this;
    }

    public InputChannelBuilder setStateWriter(ChannelStateWriter stateWriter) {
        this.stateWriter = stateWriter;
        return this;
    }

    public InputChannelBuilder setupFromNettyShuffleEnvironment(NettyShuffleEnvironment network) {
        this.partitionManager = network.getResultPartitionManager();
        this.connectionManager = network.getConnectionManager();
        this.initialBackoff = network.getConfiguration().partitionRequestInitialBackoff();
        this.maxBackoff = network.getConfiguration().partitionRequestMaxBackoff();
        this.networkBuffersPerChannel = network.getConfiguration().networkBuffersPerChannel();
        return this;
    }

    UnknownInputChannel buildUnknownChannel(SingleInputGate inputGate) {
        UnknownInputChannel channel =
                new UnknownInputChannel(
                        inputGate,
                        channelIndex,
                        partitionId,
                        subpartitionIndexSet,
                        partitionManager,
                        taskEventPublisher,
                        connectionManager,
                        initialBackoff,
                        maxBackoff,
                        partitionRequestListenerTimeout,
                        networkBuffersPerChannel,
                        metrics);
        channel.setChannelStateWriter(stateWriter);
        return channel;
    }

    public LocalInputChannel buildLocalChannel(SingleInputGate inputGate) {
        LocalInputChannel channel =
                new LocalInputChannel(
                        inputGate,
                        channelIndex,
                        partitionId,
                        subpartitionIndexSet,
                        partitionManager,
                        taskEventPublisher,
                        initialBackoff,
                        maxBackoff,
                        metrics.getNumBytesInLocalCounter(),
                        metrics.getNumBuffersInLocalCounter(),
                        stateWriter);
        markNoRecovery(channel);
        return channel;
    }

    public RemoteInputChannel buildRemoteChannel(SingleInputGate inputGate) {
        RemoteInputChannel channel =
                new RemoteInputChannel(
                        inputGate,
                        channelIndex,
                        partitionId,
                        subpartitionIndexSet,
                        connectionID,
                        connectionManager,
                        initialBackoff,
                        maxBackoff,
                        partitionRequestListenerTimeout,
                        networkBuffersPerChannel,
                        metrics.getNumBytesInRemoteCounter(),
                        metrics.getNumBuffersInRemoteCounter(),
                        stateWriter);
        markNoRecovery(channel);
        return channel;
    }

    /**
     * Same as {@link #buildLocalChannel(SingleInputGate)} but does NOT auto-mark the recovery phase
     * complete. Used by tests that want to explicitly drive the recovery push interface.
     */
    public LocalInputChannel buildLocalChannelForRecoveryTest(SingleInputGate inputGate) {
        return new LocalInputChannel(
                inputGate,
                channelIndex,
                partitionId,
                subpartitionIndexSet,
                partitionManager,
                taskEventPublisher,
                initialBackoff,
                maxBackoff,
                metrics.getNumBytesInLocalCounter(),
                metrics.getNumBuffersInLocalCounter(),
                stateWriter);
    }

    /**
     * Same as {@link #buildRemoteChannel(SingleInputGate)} but does NOT auto-mark the recovery
     * phase complete. Used by tests that want to explicitly drive the recovery push interface.
     */
    public RemoteInputChannel buildRemoteChannelForRecoveryTest(SingleInputGate inputGate) {
        return new RemoteInputChannel(
                inputGate,
                channelIndex,
                partitionId,
                subpartitionIndexSet,
                connectionID,
                connectionManager,
                initialBackoff,
                maxBackoff,
                partitionRequestListenerTimeout,
                networkBuffersPerChannel,
                metrics.getNumBytesInRemoteCounter(),
                metrics.getNumBuffersInRemoteCounter(),
                stateWriter);
    }

    /**
     * Test channels built via this builder do not undergo recovery by default; mark the recovery
     * phase complete so {@code getNextBuffer()} falls through to the live path immediately,
     * mirroring what {@code UnknownInputChannel.toLocalInputChannel/toRemoteInputChannel} does in
     * production.
     */
    private static void markNoRecovery(RecoverableInputChannel channel) {
        try {
            channel.finishRecoveredBufferDelivery();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public LocalRecoveredInputChannel buildLocalRecoveredChannel(SingleInputGate inputGate) {
        LocalRecoveredInputChannel channel =
                new LocalRecoveredInputChannel(
                        inputGate,
                        channelIndex,
                        partitionId,
                        subpartitionIndexSet,
                        partitionManager,
                        taskEventPublisher,
                        initialBackoff,
                        maxBackoff,
                        networkBuffersPerChannel,
                        metrics);
        channel.setChannelStateWriter(stateWriter);
        return channel;
    }

    public RemoteRecoveredInputChannel buildRemoteRecoveredChannel(SingleInputGate inputGate) {
        RemoteRecoveredInputChannel channel =
                new RemoteRecoveredInputChannel(
                        inputGate,
                        channelIndex,
                        partitionId,
                        subpartitionIndexSet,
                        connectionID,
                        connectionManager,
                        initialBackoff,
                        maxBackoff,
                        partitionRequestListenerTimeout,
                        networkBuffersPerChannel,
                        metrics);
        channel.setChannelStateWriter(stateWriter);
        return channel;
    }
}
