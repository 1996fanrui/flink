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
# 实施计划

-   [x] **1. 创建集成测试 (ITCase) 以复现问题**
    -   [x] 在 `flink-end-to-end-tests` 模块中，创建一个新的测试类 `UnalignedCheckpointRescaleWithMixedExchangesITCase`。
    -   [x] 实现第一个测试场景：多输出 (`Source -> (keyBy -> Map1, forward -> Map2)`)。
    -   [x] 编写执行逻辑：启用 UC -> 运行 Job -> Checkpoint -> Rescale -> 从 Checkpoint 恢复。
    -   [x] 运行测试，确认它会因为 `UnsupportedOperationException` 而失败。
    -   _需求: 需求 1, 验收标准 1, 5_

-   [x] **2. 修改 StateAssignmentOperation 核心逻辑**
    -   [x] 定位到 `flink-runtime` 模块中的 `StateAssignmentOperation.java` 文件。
    -   [x] 实现私有方法 `hasInFlightDataForChannel`，用于精确检查单个 `InputChannel` 是否有状态。
    -   [x] 修改 `reDistributeInputChannelStates` 方法，在保留全局优化的前提下，使用 `hasInFlightDataForChannel` 进行精确判断。
    -   [x] 实现类似的私有方法，用于精确检查单个 `ResultPartition` 是否有状态。
    -   [x] 修改 `reDistributeResultSubpartitionStates` 方法，应用同样的精确检查逻辑。
    -   _需求: 需求 1, 验收标准 3, 4_

-   [x] **3. 验证修复并完善测试**
    -   [x] 重新运行在任务1中失败的测试，确认它现在可以通过。
    -   [x] 在所有测试用例中加入数据正确性断言（不丢不重）。
    -   [x] 实现并运行其余三个测试场景的 ITCase (多输入, rescale 分区器, 混合复杂度)。
    -   [x] 确保所有新的集成测试都能稳定通过。
    -   _需求: 需求 1, 验收标准 1, 2, 5_

-   [x] **4. 代码格式化与最终构建**
    -   [x] 运行 `mvn spotless:apply` 格式化代码。
    -   [x] 运行相关模块的 `mvn clean install` 确保没有编译或测试问题。
    -   _需求: N/A_
