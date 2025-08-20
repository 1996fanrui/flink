<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# 需求文档

## 介绍

本文档旨在明确修复一个在 Flink 中存在的 bug：当作业从非对齐检查点（Unaligned Checkpoint, UC）恢复并进行扩缩容时，若作业中同时包含支持 UC 和不支持 UC 的分区策略，会导致恢复失败并抛出 `UnsupportedOperationException`。

问题的根源在于，在状态恢复时，系统会进行全局性的 in-flight buffer 状态检查。在混合分区策略下，这种全局检查会导致错误的判断，让系统误以为不支持 UC 的 exchange 也需要进行状态重分配。

## 需求

### 需求 1 - 对每个 Exchange 进行精确的 Buffer 状态检查

**用户故事：** 作为一个 Flink 用户，我希望我的作业在扩缩容后能够成功地从非对齐检查点（Unaligned Checkpoint）恢复，即使我的作业中包含了混合类型的分区策略（例如 `keyBy` 和 `forward`/`rescale` 的组合）。

#### 验收标准

1.  **场景：** 一个 Flink 作业包含一个源任务，该任务的输出被两个下游任务消费。
    -   一个连接使用 `keyBy`（支持 UC）。
    -   另一个连接使用不支持 UC 的分区策略（如 `forward` 或 `rescale`）。
    -   作业启用了非对齐检查点（Unaligned Checkpoint）。
    -   **While** 作业正在运行并创建了非对齐检查点, **when** 用户对作业进行扩缩容（增加或减少并行度）并从最新的检查点恢复, **the** Flink 作业 **shall** 成功恢复并继续处理数据，不会抛出 `UnsupportedOperationException`。

2.  **场景：** 一个 Flink 作业包含一个任务，该任务消费来自两个上游任务的输入。
    -   一个输入连接使用 `rebalance`（支持 UC）。
    -   另一个输入连接使用不支持 UC 的分区策略（如 `forward` 或 `rescale`）。
    -   作业启用了非对齐检查点（Unaligned Checkpoint）。
    -   **While** 作业正在运行并创建了非对齐检查点, **when** 用户对作业进行扩缩容（增加或减少并行度）并从最新的检查点恢复, **the** Flink 作业 **shall** 成功恢复并继续处理数据。

3.  **While** Flink 在 `reDistributeInputChannelStates` 方法中为某个输入 channel 进行状态重分配, **when** 判断是否需要执行重分配逻辑, **the** 系统 **shall** 仅检查与该 channel 直接相关的上游 exchange 是否有输出状态（output buffer state） **and** 仅检查与该 channel 相关的当前任务的输入 exchange 是否有输入状态（input buffer state），而不是进行全局检查。

4.  **While** Flink 在 `reDistributeResultSubpartitionStates` 方法中为某个输出 subpartition 进行状态重分配, **when** 判断是否需要执行重分配逻辑, **the** 系统 **shall** 采用与输入端类似的精确检查方式，即只检查与该 subpartition 直接相关的下游 exchange 和当前任务的输出 exchange 的状态。

5.  **When** 修复此问题, **the** 系统 **shall** 包含一个新的集成测试（ITCase），该测试覆盖上述所有场景，以确保问题的修复和防止未来回归。
