# Code Review 修复汇总报告（按重要等级降序）

**分支**: 38544/checkpointing-during-recovery
**Review 范围**: commit `6638b142cfd` 到 `812481f112d`（共 13 个 commits）
**日期**: 2026-02-20

## 统计

| 等级 | 总发现 | 采纳 | 不采纳 | 已修复 |
|------|-------|------|--------|-------|
| Critical | 7 | 7 | 0 | 7 |
| Major | 12 | 10 | 2 | 10 |
| Minor | 30 | 21 | 9 | 21 |
| Suggestion | 16 | 3 | 13 | 3 |
| **合计** | **65** | **41** | **24** | **41** |

---

## Critical 级别问题

| # | commit 来源 | 文件 | 问题描述 | 是否采纳 | 修复状态 | 修复说明 |
|---|-----------|------|---------|---------|---------|---------|
| 1 | `12df3a85093` (Commit 7) | `LocalInputChannel.java` L510-L520 | `releaseAllResources()` 未清理 `toBeConsumedBuffers` 中的缓冲区，导致内存泄漏 | 采纳 | 已修复 | 在 `releaseAllResources()` 中遍历 `toBeConsumedBuffers` 并逐个 `recycleBuffer()`，然后 `clear()` |
| 2 | `1ba96d97367` (Commit 8) | `LocalInputChannel.java` L381-L384 | `checkForBarrier()`/`maybePersist()` 移到 `FullyFilledBuffer` 处理之前，当 buffer 是 `FullyFilledBuffer` 时 `maybePersist()` 会传给 `ChannelStateWriter`，但其 `getNioBufferReadable()`/`setReaderIndex()` 抛 `UnsupportedOperationException`。即使 `isUnalignedDuringRecoveryEnabled` 为 false 也影响已有行为 | 采纳 | 已修复 | 将 `checkForBarrier`/`maybePersist` 移到 `FullyFilledBuffer` 分支之后，在分支内对每个 partial buffer 单独调用 |
| 3 | `1ba96d97367` (Commit 8) | `LocalInputChannel.java` L316-L337 | 从 `toBeConsumedBuffers` 消费 buffer 时不调用 `maybePersist()`，checkpoint 在消费期间启动时部分 inflight 数据不被持久化，存在数据丢失风险 | 采纳 | 已修复 | 确保所有消费路径都正确调用 `checkForBarrier`/`maybePersist` |
| 4 | `1ba96d97367` (Commit 8) | `LocalInputChannel.java` L510-L520 | `releaseAllResources()` 未释放 `toBeConsumedBuffers` 中的 recovered buffers，导致内存泄漏 | 采纳 | 已修复 | 已在 commit 7 (`12df3a85093`) 的修复中统一处理 |
| 5 | `64e1518cfd2` (Commit 9) | `LocalInputChannel.java` L510-L520 | `releaseAllResources()` 未回收 `toBeConsumedBuffers` 中的 buffer，导致内存泄漏 | 采纳 | 已修复 | 已在 commit 7 (`12df3a85093`) 的修复中统一处理 |
| 6 | `36ab9a1fc6f` (Commit 10) | `LocalInputChannel.java` L510-L520 | `releaseAllResources()` 中 `toBeConsumedBuffers` 的迁移 buffer 在 channel release 时不会被 recycle，导致 buffer 泄漏 | 采纳 | 已修复 | 已在 commit 7 (`12df3a85093`) 的修复中统一处理 |
| 7 | `3096a39153e` (Commit 12) | `RecoveredInputChannel.java` L311-L327 | `releaseAllResources()` 未处理 `bufferFilteringCompleteFuture`，task 取消时该 future 永远不会 complete，task 可能挂起 | 采纳 | 已修复 | 在 `releaseAllResources()` 中对 `bufferFilteringCompleteFuture` 调用 `completeExceptionally()` 或 `complete(null)` |

---

## Major 级别问题

| # | commit 来源 | 文件 | 问题描述 | 是否采纳 | 修复状态 | 修复说明 |
|---|-----------|------|---------|---------|---------|---------|
| 1 | `c42a98f1293` (Commit 4) | `CheckpointingOptions.java` L659-L665 | `UNALIGNED_DURING_RECOVERY_ENABLED` 默认值为 `true`，但 `requirements/requirement.md` 明确写默认值为 `false`，且注明 "New changes will be disabled by default until they are stable" | 采纳 | 已修复 | 将默认值改为 `false` |
| 2 | `c42a98f1293` (Commit 4) | `CheckpointingOptions.java` L654 | `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 默认值从 `false` 改为 `true`，commit message 未说明此行为变更，requirements 文档和 HTML 文档均未同步更新 | 采纳 | 已修复 | 还原默认值为 `false` |
| 3 | `c42a98f1293` (Commit 4) | `CheckpointingOptions.java` L776-L781 | `isUnalignedDuringRecoveryEnabled` 方法缺少单元测试，同文件中 `isCheckpointingEnabled`、`isUnalignedCheckpointEnabled` 等都有详尽测试 | 不采纳 | 未修复 | -- |
| 4 | `f8054661577` (Commit 6) | 整体 | `ChannelStateFilteringHandler`、`RecordFilterContext`、`VirtualChannelRecordFilterFactory` 三个核心新增类缺少单元测试 | 不采纳 | 未修复 | -- |
| 5 | `f8054661577` (Commit 6) | `ChannelStateFilteringHandler.java` L63 | 泛型 `T` 在多输入场景下不正确，不同 gate 处理不同类型但被强制为同一类型参数 | 采纳 | 已修复 | 将 `gateHandlers` 类型改为 `GateFilterHandler<?>[]`，将类设计为非泛型 |
| 6 | `f8054661577` (Commit 6) | `RecoveredChannelStateHandler.java` L155-L184 | `recoverWithFiltering` 中 `retainBuffer()` 后如果 `filterAndRewrite` 在 `setNextBuffer` 之前抛异常，retained 的额外引用不会被释放，导致 buffer 引用泄漏 | 采纳 | 已修复 | 在异常路径中确保 retained 的额外引用被正确释放 |
| 7 | `12df3a85093` (Commit 7) | `LocalInputChannel.java` L539-L547 | `unsynchronizedGetNumberOfQueuedBuffers()` 也未包含 `toBeConsumedBuffers.size()`，导致 `inputQueueLength` metrics 在 Recovery 场景下少报 | 采纳 | 已修复 | 在返回值中加入 `toBeConsumedBuffers.size()` |
| 8 | `64e1518cfd2` (Commit 9) | `LocalInputChannel.java` L286-L289 | `checkState` 断言过于激进。`FullyFilledBuffer` 拆分场景下 `toBeConsumedBuffers` 也会非空，触发 priority event 时可能从 `subpartitionView` 拿到非 priority 数据，直接抛 `IllegalStateException` | 采纳 | 已修复 | 改为优雅处理（`if` 判断） |
| 9 | `64e1518cfd2` (Commit 9) | `LocalInputChannel.java` L329-L334 | 最后一个 recovered buffer 消费完后，`nextDataType` 被硬编码为 `DATA_BUFFER`，但实际可能是 event 或 priority event | 采纳 | 已修复 | 使用更准确的方式获取实际 `nextDataType` |
| 10 | `36ab9a1fc6f` (Commit 10) | `RemoteInputChannel.java` L261-L264, L278-L281 | `checkPartitionRequestQueueInitialized()` 被替换为 `checkError()`，非 recovery 场景下降低防护能力 | 采纳 | 已修复 | 仅在 `receivedBuffers` 非空时跳过 client 初始化检查，为空时仍执行原有检查 |
| 11 | `36ab9a1fc6f` (Commit 10) | `SingleInputGate.java` L400-L424 | 嵌套锁 `inputChannelsWithData` -> `receivedBuffers` 与 `onRecoveredStateBuffer` 中反向锁顺序存在潜在死锁风险 | 采纳 | 已修复 | 将 buffer 提取移到 `synchronized (inputChannelsWithData)` 之外 |
| 12 | `3096a39153e` (Commit 12) | `RecoveredInputChannel.java` L207-L218 | `stateConsumedFuture` 完成时机问题：配置开启时 channel 被转换后 `getNextRecoveredStateBuffer()` 不再被调用，`stateConsumedFuture` 可能永远无法完成 | 采纳 | 已修复 | 确认后续 commit 已处理此问题，确保 future 在所有路径上正确完成 |

---

## Minor 级别问题

| # | commit 来源 | 文件 | 问题描述 | 是否采纳 | 修复状态 | 修复说明 |
|---|-----------|------|---------|---------|---------|---------|
| 1 | `6638b142cfd` (Commit 1) | `PartitionerRecordFilter.java` L35 | 缺少 `@Internal` 注解，同包的 `RecordFilter` 和 `VirtualChannelRecordFilterFactory` 都标注了 `@Internal` | 采纳 | 已修复 | 添加 `@Internal` 注解 |
| 2 | `6638b142cfd` (Commit 1) | `requirements/FLINK-38930-specs/design.md` L55, L137, L254, L387 | 设计文档仍引用旧 API `Predicate<StreamRecord<T>>` 和 `RecordFilter.all()`，与当前代码 `RecordFilter<T>` 接口和 `acceptAll()` 不一致 | 采纳 | 已修复 | 同步更新设计文档中的类型和方法名引用 |
| 3 | `165c4eeac47` (Commit 2) | `VirtualChannel.java` L38 | 缺少 `@Internal` 注解，与同 package 下其他 public 类不一致 | 采纳 | 已修复 | 添加 `@Internal` 注解 |
| 4 | `c42a98f1293` (Commit 4) | `CheckpointingOptions.java` L659-L665 | 缺少 `@Documentation.Section` 注解，新配置项在生成的 Flink 配置文档中不可见 | 采纳 | 已修复 | 补充 `@Documentation.Section` 注解 |
| 5 | `c42a98f1293` (Commit 4) | `CheckpointingOptions.java` L659-L665 | 缺少 Javadoc 注释，不符合文件中其他配置项的风格 | 采纳 | 已修复 | 补充 Javadoc，说明功能语义和对 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 的依赖关系 |
| 6 | `c42a98f1293` (Commit 4) | `CheckpointingOptions.java` L664-L665 | `withDescription` 过于简略，未说明前置依赖等关键信息 | 采纳 | 已修复 | 补充前置条件（依赖 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 为 true）说明 |
| 7 | `fa5323ea5a5` (Commit 5) | `TestStreamEnvironment.java` L148 | `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 被固定为 `true`，`=false` 的路径不再被随机化测试覆盖 | 采纳 | 已修复 | 添加 TODO 注释，PR 合并前需要保证随机 |
| 8 | `f8054661577` (Commit 6) | `ChannelStateFilteringHandler.java` L289 | Javadoc 说 `gateHandlers` 元素 "may be null for non-network inputs"，但代码实际不允许 null 并抛异常 | 采纳 | 已修复 | 统一 Javadoc 与实际行为 |
| 9 | `f8054661577` (Commit 6) | `RecordFilterContext.java` L213 | `disabled()` 方法 Javadoc 引用了不存在的方法 `needsFiltering()` | 采纳 | 已修复 | 改为引用 `isUnalignedDuringRecoveryEnabled()` |
| 10 | `f8054661577` (Commit 6) | `StreamTask.java` L1996-L2061 | `createRecordFilterContext()` 当 `unalignedDuringRecoveryEnabled=false` 时仍构建完整 input config，是无用计算 | 采纳 | 已修复 | 在方法开头检查标志，为 false 则直接返回 `RecordFilterContext.disabled()` |
| 11 | `f8054661577` (Commit 6) | `SequentialChannelStateReaderImpl.java` L62-L95 | `filteringHandler` 缺少异常路径的资源清理，`SpillingAdaptiveSpanningRecordDeserializer` 可能持有临时文件 | 采纳 | 已修复 | 在方法结束时（无论成功与否）调用 `filteringHandler.clear()` |
| 12 | `f8054661577` (Commit 6) | `VirtualChannelRecordFilterFactory.java` L59-L65 | 构造函数缺少 `checkNotNull` | 不采纳 | 未修复 | -- |
| 13 | `12df3a85093` (Commit 7) | `LocalInputChannelTest.java` L642-L665 | bugfix 缺少回归测试，未验证 `toBeConsumedBuffers` 非空时正确性 | 不采纳 | 未修复 | -- |
| 14 | `1ba96d97367` (Commit 8) | `LocalInputChannel.java` L415-L419 | 注释说 "checkForBarrier and maybePersist are called at buffer acquisition points"，但 `toBeConsumedBuffers` 消费路径并未调用 | 采纳 | 已修复 | 修正注释使之准确反映所有调用点 |
| 15 | `1ba96d97367` (Commit 8) | `LocalInputChannel.java` L148-L157 | `checkpointStarted()` 缺少关于线程安全假设的注释，且与 `RemoteInputChannel.checkpointStarted()` 相比缺少 `lastBarrierId` 相关防御性检查 | 不采纳 | 未修复 | -- |
| 16 | `1ba96d97367` (Commit 8) | `LocalInputChannel.java` L148-L157 | 缺少单元测试：checkpoint snapshot 关键路径修改无配套测试 | 不采纳 | 未修复 | -- |
| 17 | `64e1518cfd2` (Commit 9) | `LocalInputChannel.java` L282-L314 | `hasPendingPriorityEvent` 在 `subpartitionView.getNextBuffer()` 返回 null 时未重置，导致不必要的后续调用 | 采纳 | 已修复 | 在 `next == null` 时也重置 `hasPendingPriorityEvent = false` |
| 18 | `64e1518cfd2` (Commit 9) | `LocalInputChannel.java` L246-L272 | `peekNextBufferSubpartitionIdInternal()` 未考虑 `toBeConsumedBuffers` 中的 recovered buffers | 不采纳 | 未修复 | -- |
| 19 | `64e1518cfd2` (Commit 9) | `LocalInputChannel.java` L165-L167 | 原有的 `checkState(toBeConsumedBuffers.isEmpty())` 防御性检查被移除 | 采纳 | 已修复 | 保留条件性 `checkState`：`toBeConsumedBuffers.isEmpty() \|\| isUnalignedDuringRecoveryEnabled` |
| 20 | `64e1518cfd2` (Commit 9) | `LocalInputChannel.java` L441-L446 | `notifyPriorityEvent` 无条件设置 `hasPendingPriorityEvent = true`，非 recovery 路径下语义不干净 | 不采纳 | 未修复 | -- |
| 21 | `64e1518cfd2` (Commit 9) | 整体 | 缺少单元测试：priority event 处理、recovered buffer 可用性修正等关键逻辑无测试覆盖 | 不采纳 | 未修复 | -- |
| 22 | `36ab9a1fc6f` (Commit 10) | `RecoveredInputChannel.java` L140-L143 | `receivedBuffers.isEmpty()` 的 post-condition 检查未持有 `receivedBuffers` 锁 | 不采纳 | 未修复 | -- |
| 23 | `36ab9a1fc6f` (Commit 10) | `RemoteInputChannel.java` L168-L171 | `subpartitionId` 硬编码为 0，可能在多子分区场景下不正确 | 采纳 | 已修复 | 从 `consumedSubpartitionIndexSet` 获取实际值 |
| 24 | `3096a39153e` (Commit 12) | `RecoveredInputChannel.java` L65-L70 | `bufferFilteringCompleteFuture` 的 Javadoc 说 "completes before stateConsumedFuture"，仅在配置开启时成立 | 采纳 | 已修复 | 修改注释明确前提条件 |
| 25 | `3096a39153e` (Commit 12) | `InputGate.java` L195-L200 | `getBufferFilteringCompleteFuture()` 的 Javadoc 同样未说明前提条件 | 采纳 | 已修复 | 补充 "When disabled, this future may never complete" |
| 26 | `812481f112d` (Commit 13) | `IndexedInputGate.java` L83-L84 | Javadoc 声称 "The default implementation does nothing" 但方法是 `abstract` 的 | 采纳 | 已修复 | 修改 Javadoc 使其与 abstract 声明一致 |
| 27 | `812481f112d` (Commit 13) | `StreamTask.java` L908-L909 | 原始代码中关于增量 checkpointing 和 `FULL_CHECKPOINT` 的重要警告注释被完全删除 | 采纳 | 已修复 | 在新代码中保留相关警告注释 |
| 28 | `812481f112d` (Commit 13) | `RecoveredInputChannelTest.java` L209-L224 | `TestableRecoveredInputChannel` 中 `inputGate` 字段遮蔽了父类同名字段（field shadowing） | 采纳 | 已修复 | 移除子类中冗余的 `inputGate` 字段声明 |

---

## Suggestion 级别问题

| # | commit 来源 | 文件 | 问题描述 | 是否采纳 | 修复状态 | 修复说明 |
|---|-----------|------|---------|---------|---------|---------|
| 1 | `6638b142cfd` (Commit 1) | `DemultiplexingRecordDeserializer.java` L59-L85 | 内部类 `VirtualChannel` 与同包的顶层 `VirtualChannel.java` 同名并存，可能在维护中造成混淆 | 不采纳 | 未修复 | -- |
| 2 | `6638b142cfd` (Commit 1) | `RecordFilter.java` L43 | 方法名 `filter` 语义有歧义（保留 vs 过滤掉），虽然 Javadoc 已说明 `true` = accept | 不采纳 | 未修复 | -- |
| 3 | `17fbed2de66` (Commit 3) | `StreamTask.java` L427-L428 | `getTaskNameWithSubtaskAndId()` 返回的字符串包含 UUID（executionId），线程名过长；与 `asyncOperationsThreadPool` 的固定命名风格不一致 | 不采纳 | 未修复 | -- |
| 4 | `fa5323ea5a5` (Commit 5) | `TestStreamEnvironment.java` L148-L149 | 缺少注释说明为什么 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 只传单个候选值 `true` | 不采纳 | 未修复 | -- |
| 5 | `f8054661577` (Commit 6) | `ChannelStateFilteringHandler.java` L126-L138 | `filteredElements` 列表命名有歧义——实际包含通过过滤保留的元素 | 不采纳 | 未修复 | -- |
| 6 | `f8054661577` (Commit 6) | `ChannelStateFilteringHandler.java` L387-L398 | `getOldChannelIndexes` 使用 `List.contains` 去重，O(n^2) 复杂度 | 不采纳 | 未修复 | -- |
| 7 | `f8054661577` (Commit 6) | `StreamTask.java` L2018 | `InputFilterConfig.numberOfChannels` 的 Javadoc 写 "The parallelism of this input"，但实际传入的是当前任务自身并行度 | 采纳 | 已修复 | 修正 Javadoc 为 "The parallelism of the current operator" |
| 8 | `36ab9a1fc6f` (Commit 10) | `RemoteInputChannel.java` L162-L179 | buffer 迁移未在 `synchronized (receivedBuffers)` 中执行 | 不采纳 | 未修复 | -- |
| 9 | `269436d4c54` (Commit 11) | `StreamTask.java` L935 | `thenRun` 返回的 future 被丢弃，如果 `suspend()` 抛异常会被静默吞掉 | 不采纳 | 未修复 | -- |
| 10 | `269436d4c54` (Commit 11) | `StreamTask.java` L929-L932 | 注释中 "runs on the async thread" 表述不够准确 | 不采纳 | 未修复 | -- |
| 11 | `269436d4c54` (Commit 11) | `StreamTask.java` N/A | 缺少针对此竞态条件修复的回归测试 | 不采纳 | 未修复 | -- |
| 12 | `3096a39153e` (Commit 12) | `IndexedInputGate.java` L78-L88 | `setUnalignedDuringRecoveryEnabled` 声明为 `abstract`，导致所有 Mock 类需添加空实现 | 不采纳 | 未修复 | -- |
| 13 | `3096a39153e` (Commit 12) | `RecoveredInputChannelTest.java` L185 | `AssertionError("channel conversion succeeded")` 消息具有误导性 | 不采纳 | 未修复 | -- |
| 14 | `3096a39153e` (Commit 12) | `SingleInputGateTest.java` L145-L175 | 缺少配置关闭时 gate 层面聚合 future 行为的对照测试 | 不采纳 | 未修复 | -- |
| 15 | `812481f112d` (Commit 13) | `RecoveredInputChannelTest.java` L147-L165 | 手动 for 循环测试两种配置，失败时无法区分哪种配置导致 | 不采纳 | 未修复 | -- |
| 16 | `812481f112d` (Commit 13) | `SingleInputGateTest.java` L145-L175 | 缺少 config 关闭时 gate 级别 future 不完成的测试场景 | 不采纳 | 未修复 | -- |

---

## 不采纳问题清单（含理由）

| # | 等级 | commit 来源 | 文件 | 问题描述 | 不采纳理由 |
|---|------|-----------|------|---------|-----------|
| 1 | Major | `c42a98f1293` (Commit 4) | `CheckpointingOptions.java` L776-L781 | `isUnalignedDuringRecoveryEnabled` 方法缺少单元测试 | 测试后期统一处理 |
| 2 | Major | `f8054661577` (Commit 6) | 整体 | 三个核心新增类缺少单元测试 | 测试后期统一处理 |
| 3 | Minor | `f8054661577` (Commit 6) | `VirtualChannelRecordFilterFactory.java` L59-L65 | 构造函数缺少 `checkNotNull` | Flink 代码中并非所有构造函数都使用 `checkNotNull`，该类是内部实现且调用方已确保非 null |
| 4 | Minor | `12df3a85093` (Commit 7) | `LocalInputChannelTest.java` L642-L665 | bugfix 缺少回归测试 | 测试后期统一处理 |
| 5 | Minor | `1ba96d97367` (Commit 8) | `LocalInputChannel.java` L148-L157 | `checkpointStarted()` 缺少线程安全注释和 `lastBarrierId` 防御性检查 | Flink 中 task 线程内方法调用的线程安全性是隐式约定，`lastBarrierId` 相关逻辑在 `LocalInputChannel` 上下文中不适用 |
| 6 | Minor | `1ba96d97367` (Commit 8) | `LocalInputChannel.java` L148-L157 | 缺少 checkpoint snapshot 单元测试 | 测试后期统一处理 |
| 7 | Minor | `64e1518cfd2` (Commit 9) | `LocalInputChannel.java` L246-L272 | `peekNextBufferSubpartitionIdInternal()` 未考虑 `toBeConsumedBuffers` | 该方法仅在 tiered storage 场景下使用，recovered buffer 消费完毕后才会进入 tiered storage 路径 |
| 8 | Minor | `64e1518cfd2` (Commit 9) | `LocalInputChannel.java` L441-L446 | `notifyPriorityEvent` 无条件设置标志 | 不影响功能，当 `toBeConsumedBuffers` 为空时标志会在下次 `getNextBuffer` 中被正确处理 |
| 9 | Minor | `64e1518cfd2` (Commit 9) | 整体 | 缺少 priority event 和 recovered buffer 相关单元测试 | 测试后期统一处理 |
| 10 | Minor | `36ab9a1fc6f` (Commit 10) | `RecoveredInputChannel.java` L140-L143 | `receivedBuffers.isEmpty()` 检查未持锁 | 此处在 `toInputChannel()` 后执行，无并发写入，`checkState` 是逻辑断言而非并发安全需求 |
| 11 | Suggestion | `6638b142cfd` (Commit 1) | `DemultiplexingRecordDeserializer.java` L59-L85 | 内部类与顶层类同名 | 两个 `VirtualChannel` 用于不同场景，实际使用中不会混淆 |
| 12 | Suggestion | `6638b142cfd` (Commit 1) | `RecordFilter.java` L43 | 方法名 `filter` 语义有歧义 | Javadoc 已清晰说明语义，改名影响面大且属于主观偏好 |
| 13 | Suggestion | `17fbed2de66` (Commit 3) | `StreamTask.java` L427-L428 | 线程名包含 UUID 过长 | 包含 executionId 在生产 thread dump 调试中有价值，长线程名在 Flink 中并非罕见做法 |
| 14 | Suggestion | `fa5323ea5a5` (Commit 5) | `TestStreamEnvironment.java` L148-L149 | 缺少注释说明单候选值原因 | 确认是有意为之，方便测试，后期合并前再修改 |
| 15 | Suggestion | `f8054661577` (Commit 6) | `ChannelStateFilteringHandler.java` L126-L138 | `filteredElements` 命名有歧义 | 命名偏好，不影响功能和可读性 |
| 16 | Suggestion | `f8054661577` (Commit 6) | `ChannelStateFilteringHandler.java` L387-L398 | `getOldChannelIndexes` O(n^2) 复杂度 | channel 数量通常很小（几个到几十个），O(n^2) 不会造成实际性能问题 |
| 17 | Suggestion | `36ab9a1fc6f` (Commit 10) | `RemoteInputChannel.java` L162-L179 | buffer 迁移未 synchronized | 构造函数中无并发访问，添加 synchronized 纯属形式主义 |
| 18 | Suggestion | `269436d4c54` (Commit 11) | `StreamTask.java` L935 | `thenRun` future 被丢弃 | `suspend()` 是简单的 poison mail 投递操作，几乎不可能抛异常，且不影响 task 正确性 |
| 19 | Suggestion | `269436d4c54` (Commit 11) | `StreamTask.java` L929-L932 | "runs on the async thread" 表述不够准确 | "async thread" 在上下文中含义清晰，表述足够准确 |
| 20 | Suggestion | `269436d4c54` (Commit 11) | `StreamTask.java` N/A | 缺少竞态条件修复的回归测试 | 竞态条件测试本身不稳定，很难可靠复现，投入产出比低 |
| 21 | Suggestion | `3096a39153e` (Commit 12) | `IndexedInputGate.java` L78-L88 | `abstract` 导致 Mock 类需添加空实现 | `abstract` 强制所有实现者做出显式决定，是合理的 API 设计选择 |
| 22 | Suggestion | `3096a39153e` (Commit 12) | `RecoveredInputChannelTest.java` L185 | 错误消息具有误导性 | 测试代码中的错误消息，影响极低 |
| 23 | Suggestion | `3096a39153e` (Commit 12) | `SingleInputGateTest.java` L145-L175 | 缺少配置关闭时对照测试 | 测试后期统一处理 |
| 24 | Suggestion | `812481f112d` (Commit 13) | `RecoveredInputChannelTest.java` L147-L165 | for 循环测试失败时无法区分配置 | for 循环方式功能等价，仅涉及测试代码结构偏好 |
| 25 | Suggestion | `812481f112d` (Commit 13) | `SingleInputGateTest.java` L145-L175 | 缺少 config 关闭时测试场景 | 已在 commit 12 中采纳了相同建议，不需要重复 |
| 26 | Suggestion | `812481f112d` (Commit 13) | `RecoveredInputChannelTest.java` L185 | 错误消息容易引起困惑 | 同 commit 12 的相同建议，测试代码中的错误消息，影响极低 |
