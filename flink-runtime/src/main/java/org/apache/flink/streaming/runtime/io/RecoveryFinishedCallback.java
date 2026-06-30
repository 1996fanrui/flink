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

/**
 * Notified once an input has fully consumed its channel-state recovery data, i.e. on the aggregated
 * {@code END_OF_RECOVERY} signal after every channel has left recovery. Lets the owning task retire
 * its recovery-checkpoint trigger at the correct moment.
 */
@Internal
@FunctionalInterface
public interface RecoveryFinishedCallback {

    RecoveryFinishedCallback NO_OP = () -> {};

    void onRecoveryFinished();
}
