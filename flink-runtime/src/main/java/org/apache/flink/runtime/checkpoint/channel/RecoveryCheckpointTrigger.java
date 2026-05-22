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

import java.io.IOException;

/**
 * Implemented by {@code SpillFileReader}; the task thread holds the reference typed as this
 * interface. Drives Step 1 of the recovery-checkpoint protocol.
 */
@Internal
public interface RecoveryCheckpointTrigger {

    /**
     * Step 1 of the recovery-checkpoint protocol. Atomically:
     *
     * <ol>
     *   <li>Enters {@code SpillFileReader.lock}.
     *   <li>Snapshots every SpillFileSegment and captures {@code (currentSegmentIndex,
     *       currentOffset)} as {@link DiskSnapshot.StartPos}.
     *   <li>Calls {@code onRecoveredStateBuffer(new RecoveryCheckpointBarrier(checkpointId))} on
     *       every channel of the task.
     *   <li>Leaves the lock.
     * </ol>
     *
     * <p>The {@code checkpointId} is forwarded from {@code CheckpointBarrier.getId()} by the
     * checkpoint dispatcher; it is embedded in the {@link RecoveryCheckpointBarrier} sentinel so
     * that Step 2 can correlate the barrier with the triggering checkpoint.
     *
     * <p>Caller (task thread) MUST NOT hold {@code SpillFileReader.lock} — the implementation takes
     * the lock itself.
     *
     * <p>The returned {@link DiskSnapshot} feeds into {@link
     * ChannelStateWriter#addInputDataFromSpill} at Step 3.
     *
     * @param checkpointId the id of the checkpoint being triggered
     * @return a snapshot of the disk state at the moment of the atomic cut
     */
    DiskSnapshot snapshotAndInsertBarriers(long checkpointId) throws IOException;

    /**
     * No-op singleton used on the feature-off path and once recovery has fully completed. Returning
     * the empty {@link DiskSnapshot} singleton lets the dispatcher run Step 1 / Step 3
     * unconditionally — there is no {@code if (filter-on)} branch at the dispatcher layer.
     */
    RecoveryCheckpointTrigger NO_OP = checkpointId -> DiskSnapshot.empty();
}
