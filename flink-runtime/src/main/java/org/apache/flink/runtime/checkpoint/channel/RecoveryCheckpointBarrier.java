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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;

/**
 * Task-local sentinel event carrying the checkpoint id that triggered it. Wrapped into a Buffer and
 * inserted into each channel's {@code recoveredBuffers} queue to mark the cut between recovered
 * state delivered before vs. after a recovery-checkpoint trigger; consumers match by {@link
 * #getCheckpointId()}.
 *
 * <p>This event never travels across the network; it is created and consumed within a single task,
 * but goes through serialize/deserialize because the channel queue carries {@code Buffer} only.
 */
@Internal
public final class RecoveryCheckpointBarrier extends RuntimeEvent {

    private long checkpointId;

    public RecoveryCheckpointBarrier(long checkpointId) {
        this.checkpointId = checkpointId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        out.writeLong(checkpointId);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        this.checkpointId = in.readLong();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(checkpointId);
    }

    @Override
    public boolean equals(Object other) {
        return other != null
                && other.getClass() == RecoveryCheckpointBarrier.class
                && ((RecoveryCheckpointBarrier) other).checkpointId == this.checkpointId;
    }

    @Override
    public String toString() {
        return "RecoveryCheckpointBarrier(" + checkpointId + ")";
    }
}
