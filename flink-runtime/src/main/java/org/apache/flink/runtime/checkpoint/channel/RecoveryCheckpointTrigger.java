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
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;

/**
 * Inserts recovery-checkpoint barriers into in-flight recovered state. The task thread holds the
 * reference typed as this interface; the production implementation lives in {@code
 * SpillFileDrainer}.
 */
@Internal
public interface RecoveryCheckpointTrigger {

    /**
     * Atomically, under the drainer's lock:
     *
     * <ol>
     *   <li>Derives a sub-{@link SpillFileReader} from the drain reader's current cursor, covering
     *       all entries the drain has not yet delivered to channels.
     *   <li>Calls {@code onRecoveredStateBuffer(new RecoveryCheckpointBarrier(checkpointId))} on
     *       every in-recovery channel of the task.
     * </ol>
     *
     * <p>Returns the sub-reader (wrapped as a {@link CloseableIterator}) so the checkpoint write
     * path can stream the entries directly. When no entries remain, returns {@link
     * CloseableIterator#empty()}.
     *
     * <p>The {@code checkpointId} is forwarded from {@code CheckpointBarrier.getId()} and embedded
     * in the {@link RecoveryCheckpointBarrier} sentinel so consumers can correlate the barrier with
     * the triggering checkpoint.
     */
    CloseableIterator<SpillFileReader.Chunk> snapshotAndInsertBarriers(long checkpointId)
            throws IOException;

    /**
     * No-op singleton used when no spill file exists or recovery has fully completed. Returning an
     * empty iterator lets the checkpoint dispatcher invoke this unconditionally without branching
     * on whether recovery filtering is active.
     */
    RecoveryCheckpointTrigger NO_OP = checkpointId -> CloseableIterator.empty();
}
