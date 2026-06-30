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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Fans the per-input {@code END_OF_RECOVERY} signals of a multi-input task into a single delegate
 * call, fired only once the last input has finished recovery. Shared by all of a task's
 * recoverable inputs so the task-level recovery-checkpoint trigger is retired only after every
 * input has actually left recovery (retiring on the first would reopen the premature-swap window
 * for the still-recovering inputs).
 *
 * <p>Not thread-safe: every input's {@code processInput} runs on the single task thread, so the
 * counter needs no synchronization.
 */
@Internal
final class CountdownRecoveryFinishedCallback implements RecoveryFinishedCallback {

    private final RecoveryFinishedCallback onAllInputsFinished;
    private int remaining;

    CountdownRecoveryFinishedCallback(
            int recoverableInputCount, RecoveryFinishedCallback onAllInputsFinished) {
        checkArgument(
                recoverableInputCount > 0,
                "recoverableInputCount must be positive but was %s",
                recoverableInputCount);
        this.remaining = recoverableInputCount;
        this.onAllInputsFinished = checkNotNull(onAllInputsFinished);
    }

    @Override
    public void onRecoveryFinished() {
        checkState(remaining > 0, "onRecoveryFinished called more times than recoverable inputs");
        if (--remaining == 0) {
            onAllInputsFinished.onRecoveryFinished();
        }
    }
}
