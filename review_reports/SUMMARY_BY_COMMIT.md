# Code Review Summary (By Commit)

**Review 范围**: commit `6638b142cfd` 到 `812481f112d`（共 13 个 commits）
**分支**: `38544/checkpointing-during-recovery`
**Review 日期**: 2026-02-20

**采纳标记说明**: `[x]` = 建议采纳，`[ ]` = 建议不采纳（附理由）
**修复标记说明**: `[x]` = 已修复，`[ ]` = 未修复

---

## Commit 1: `6638b142cfd` — [hotfix] Extract RecordFilter as the interface

**Review 结论**: 通过（有小建议）

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [x] | [x] | Minor | `6638b142cfd` | `PartitionerRecordFilter.java` | L35 | 缺少 `@Internal` 注解，同包的 `RecordFilter` 和 `VirtualChannelRecordFilterFactory` 都标注了 `@Internal` | 添加 `@Internal` 注解 |
| [x] | [x] | Minor | `6638b142cfd` | `requirements/FLINK-38930-specs/design.md` | L55, L137, L254, L387 | 设计文档仍引用旧 API `Predicate<StreamRecord<T>>` 和 `RecordFilter.all()`，与当前代码 `RecordFilter<T>` 接口和 `acceptAll()` 不一致 | 同步更新设计文档中的类型和方法名引用 |
| [ ] | [ ] | Suggestion | `6638b142cfd` | `DemultiplexingRecordDeserializer.java` | L59-L85 | 内部类 `VirtualChannel` 与同包的顶层 `VirtualChannel.java` 同名并存，可能在维护中造成混淆 | 确认是否有统一计划，或在 Javadoc 中说明各自使用场景 |
| [ ] | [ ] | Suggestion | `6638b142cfd` | `RecordFilter.java` | L43 | 方法名 `filter` 语义有歧义（保留 vs 过滤掉），虽然 Javadoc 已说明 `true` = accept | 考虑使用更明确的命名如 `shouldAccept` 或 `matches` |

> **不采纳理由**:
> - #3: 两个 `VirtualChannel` 是不同类用于不同场景（`DemultiplexingRecordDeserializer` 内部的用于 rescaling 反序列化，顶层的用于 filtering），实际使用中不会混淆
> - #4: Javadoc 已清晰说明语义，改名影响面大且属于主观偏好

---

## Commit 2: `165c4eeac47` — [hotfix] Extract VirtualChannel as the public class

**Review 结论**: 通过（Minor 问题）

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [x] | [x] | Minor | `165c4eeac47` | `VirtualChannel.java` | L38 | 缺少 `@Internal` 注解，与同 package 下其他 public 类不一致 | 在 `public class VirtualChannel<T>` 上方添加 `@Internal` 注解 |

---

## Commit 3: `17fbed2de66` — [hotfix] Including task name and subtask index into channel-state-unspilling thread name

**Review 结论**: 通过

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [ ] | [ ] | Suggestion | `17fbed2de66` | `StreamTask.java` | L427-L428 | `getTaskNameWithSubtaskAndId()` 返回的字符串包含 UUID（executionId），线程名过长；与 `asyncOperationsThreadPool` 的固定命名风格不一致 | 考虑使用 `getTaskNameWithSubtasks()`（不含 executionId） |

> **不采纳理由**: 包含 executionId 在生产 thread dump 调试中有价值（可区分同一 subtask 的不同 attempt），长线程名在 Flink 中并非罕见做法

---

## Commit 4: `c42a98f1293` — [FLINK-38541][checkpoint] Introducing config option: execution.checkpointing.unaligned.during-recovery.enabled

**Review 结论**: 需要修改

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [x] | [x] | Major | `c42a98f1293` | `CheckpointingOptions.java` | L659-L665 | `UNALIGNED_DURING_RECOVERY_ENABLED` 默认值为 `true`，但 `requirements/requirement.md` 第 98 行明确写默认值为 `false`，且第 100 行注明 "New changes will be disabled by default until they are stable" | 将默认值改为 `false` |
| [x] | [x] | Major | `c42a98f1293` | `CheckpointingOptions.java` | L654 | `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 默认值从 `false` 改为 `true`，commit message 未说明此行为变更，requirements 文档和 HTML 文档均未同步更新 | 请还原成 false |
| [ ] | [ ] | Major | `c42a98f1293` | `CheckpointingOptions.java` | L776-L781 | `isUnalignedDuringRecoveryEnabled` 方法缺少单元测试，而同文件中 `isCheckpointingEnabled`、`isUnalignedCheckpointEnabled` 等都有详尽测试 | 补充覆盖所有配置组合（4 种）的单元测试 |
| [x] | [x] | Minor | `c42a98f1293` | `CheckpointingOptions.java` | L659-L665 | 缺少 `@Documentation.Section` 注解，新配置项在生成的 Flink 配置文档中不可见 | 补充 `@Documentation.Section` 注解 |
| [x] | [x] | Minor | `c42a98f1293` | `CheckpointingOptions.java` | L659-L665 | 缺少 Javadoc 注释，不符合文件中其他配置项的风格 | 补充 Javadoc，说明功能语义和对 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 的依赖关系 |
| [x] | [x] | Minor | `c42a98f1293` | `CheckpointingOptions.java` | L664-L665 | `withDescription` 过于简略，未说明前置依赖等关键信息 | 补充前置条件（依赖 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 为 true）说明 |

> **不采纳理由**:
> - #3 (单元测试): 测试后期统一处理

---

## Commit 5: `fa5323ea5a5` — [FLINK-38541][checkpoint] Randomize UNALIGNED_DURING_RECOVERY_ENABLED for testing

**Review 结论**: 通过（有小建议）

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [X] | [x] | Minor | `fa5323ea5a5` | `TestStreamEnvironment.java` | L148 | `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 被固定为 `true`，`=false` 的路径不再被随机化测试覆盖 | 确认是否有意为之，方便测试，后期合并前再修改，可以加一个 TODO，PR 合并前需要保证随机 |
| [] | [ ] | Suggestion | `fa5323ea5a5` | `TestStreamEnvironment.java` | L148-L149 | 缺少注释说明为什么 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM` 只传单个候选值 `true` | 添加注释说明：该选项是 `UNALIGNED_DURING_RECOVERY_ENABLED` 的前置依赖 |

> **不采纳理由**: 确认是否有意为之，方便测试，后期合并前再修改，可以加一个 TODO，PR 合并前需要保证随机

---

## Commit 6: `f8054661577` — [FLINK-38930][checkpoint] Filtering record before processing without spilling strategy

**Review 结论**: 需要修改

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [ ] | [ ] | Major | `f8054661577` | 整体 | N/A | `ChannelStateFilteringHandler`、`RecordFilterContext`、`VirtualChannelRecordFilterFactory` 三个核心新增类缺少单元测试 | 添加覆盖过滤路径、边界情况、spanning record 等场景的单元测试 |
| [x] | [ ] | Major | `f8054661577` | `ChannelStateFilteringHandler.java` | L63 | 泛型 `T` 在多输入场景下不正确，不同 gate 处理不同类型但被强制为同一类型参数 | 将 `gateHandlers` 类型改为 `GateFilterHandler<?>[]`，或将类设计为非泛型 |
| [x] | [ ] | Major | `f8054661577` | `RecoveredChannelStateHandler.java` | L155-L184 | `recoverWithFiltering` 中 `retainBuffer()` 后如果 `filterAndRewrite` 在 `setNextBuffer` 之前抛异常，retained 的额外引用不会被释放，导致 buffer 引用泄漏 | 在异常路径中（catch 或 finally）确保 retained 的额外引用被正确释放 |
| [x] | [ ] | Minor | `f8054661577` | `ChannelStateFilteringHandler.java` | L289 | Javadoc 说 `gateHandlers` 元素 "may be null for non-network inputs"，但代码实际不允许 null 并抛异常 | 统一 Javadoc 与实际行为 |
| [x] | [ ] | Minor | `f8054661577` | `RecordFilterContext.java` | L213 | `disabled()` 方法 Javadoc 引用了不存在的方法 `needsFiltering()` | 改为 `isUnalignedDuringRecoveryEnabled()` 或删除引用 |
| [x] | [ ] | Minor | `f8054661577` | `StreamTask.java` | L1996-L2061 | `createRecordFilterContext()` 当 `unalignedDuringRecoveryEnabled=false` 时仍构建完整 input config，是无用计算 | 在方法开头检查标志，为 false 则直接返回 `RecordFilterContext.disabled()` |
| [x] | [ ] | Minor | `f8054661577` | `SequentialChannelStateReaderImpl.java` | L62-L95 | `filteringHandler` 缺少异常路径的资源清理，`SpillingAdaptiveSpanningRecordDeserializer` 可能持有临时文件 | 在方法结束时（无论成功与否）调用 `filteringHandler.clear()` |
| [ ] | [ ] | Minor | `f8054661577` | `VirtualChannelRecordFilterFactory.java` | L59-L65 | 构造函数缺少 `checkNotNull` | 添加 null 检查 |
| [ ] | [ ] | Suggestion | `f8054661577` | `ChannelStateFilteringHandler.java` | L126-L138 | `filteredElements` 列表命名有歧义——实际包含通过过滤**保留**的元素 | 改为 `acceptedElements` 或 `keptElements` |
| [ ] | [ ] | Suggestion | `f8054661577` | `ChannelStateFilteringHandler.java` | L387-L398 | `getOldChannelIndexes` 使用 `List.contains` 去重，O(n^2) 复杂度 | 使用 `LinkedHashSet<Integer>` 替代 |
| [x] | [ ] | Suggestion | `f8054661577` | `StreamTask.java` | L2018 | `InputFilterConfig.numberOfChannels` 的 Javadoc 写 "The parallelism of this input"，但实际传入的是当前任务自身并行度 | 修正 Javadoc 为 "The parallelism of the current operator" |

> **不采纳理由**:
> - #1 (单元测试): 测试后期统一处理
> - #8 (`checkNotNull`): Flink 代码中并非所有构造函数都使用 `checkNotNull`，该类是内部实现且调用方已确保非 null，非必须
> - #9 (`filteredElements` 命名): 命名偏好，不影响功能和可读性
> - #10 (O(n^2)): channel 数量通常很小（几个到几十个），O(n^2) 不会造成实际性能问题

---

## Commit 7: `12df3a85093` — [hotfix] Fix LocalInputChannel.getBuffersInUseCount to include toBeConsumedBuffers

**Review 结论**: 需要修改

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [x] | [ ] | Critical | `12df3a85093` | `LocalInputChannel.java` | L510-L520 | `releaseAllResources()` 未清理 `toBeConsumedBuffers` 中的缓冲区，导致内存泄漏 | 在 `releaseAllResources()` 中遍历 `toBeConsumedBuffers` 并逐个 `recycleBuffer()`，然后 `clear()` |
| [x] | [ ] | Major | `12df3a85093` | `LocalInputChannel.java` | L539-L547 | `unsynchronizedGetNumberOfQueuedBuffers()` 也未包含 `toBeConsumedBuffers.size()`，导致 `inputQueueLength` metrics 在 Recovery 场景下少报 | 在返回值中加入 `toBeConsumedBuffers.size()` |
| [ ] | [ ] | Minor | `12df3a85093` | `LocalInputChannelTest.java` | L642-L665 | bugfix 缺少回归测试，未验证 `toBeConsumedBuffers` 非空时正确性 | 添加测试：传入非空 `initialRecoveredBuffers` 后断言 `getBuffersInUseCount()` 正确 |

> **不采纳理由**:
> - #3 (回归测试): 测试后期统一处理

---

## Commit 8: `1ba96d97367` — [FLINK-39018][checkpoint] Support LocalInputChannel checkpoint snapshot for recovered buffers

**Review 结论**: 需要修改

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [x] | [ ] | Critical | `1ba96d97367` | `LocalInputChannel.java` | L381-L384 | `checkForBarrier()`/`maybePersist()` 移到 `FullyFilledBuffer` 处理之前。当 buffer 是 `FullyFilledBuffer` 时，`maybePersist()` 会将整个 `FullyFilledBuffer` 传给 `ChannelStateWriter`，但其 `getNioBufferReadable()`/`setReaderIndex()` 抛 `UnsupportedOperationException`。**即使 `isUnalignedDuringRecoveryEnabled` 为 false 也影响已有行为** | 将 `checkForBarrier`/`maybePersist` 移到 `FullyFilledBuffer` 分支之后，或在分支内对每个 partial buffer 单独调用 |
| [x] | [ ] | Critical | `1ba96d97367` | `LocalInputChannel.java` | L316-L337 | 从 `toBeConsumedBuffers` 消费 buffer 时不调用 `maybePersist()`。checkpoint 在消费期间启动时，部分 inflight 数据不被持久化，**存在数据丢失风险** | 确保所有消费路径都正确调用 `checkForBarrier`/`maybePersist` |
| [x] | [ ] | Critical | `1ba96d97367` | `LocalInputChannel.java` | L510-L520 | `releaseAllResources()` 未释放 `toBeConsumedBuffers` 中的 recovered buffers，导致内存泄漏 | 在 `releaseAllResources()` 中遍历并 recycle |
| [x] | [ ] | Minor | `1ba96d97367` | `LocalInputChannel.java` | L415-L419 | 注释说 "checkForBarrier and maybePersist are called at buffer acquisition points"，但 `toBeConsumedBuffers` 消费路径并未调用 | 修正注释使之准确反映所有调用点 |
| [ ] | [ ] | Minor | `1ba96d97367` | `LocalInputChannel.java` | L148-L157 | `checkpointStarted()` 缺少关于线程安全假设的注释，且与 `RemoteInputChannel.checkpointStarted()` 相比缺少 `lastBarrierId` 相关防御性检查 | 添加线程安全注释；确认是否需要 `lastBarrier` 相关 reset 逻辑 |
| [ ] | [ ] | Minor | `1ba96d97367` | `LocalInputChannel.java` | L148-L157 | 缺少单元测试：checkpoint snapshot 关键路径修改无配套测试 | 添加测试覆盖 `toBeConsumedBuffers` 非空时 checkpoint 行为 |

> **不采纳理由**:
> - #5 (线程安全注释): Flink 中 task 线程内方法调用的线程安全性是隐式约定，`LocalInputChannel` 作为非线程安全组件不需要在每个方法上标注。`lastBarrierId` 相关逻辑在 `LocalInputChannel` 的上下文中不适用（`RemoteInputChannel` 有特殊的 barrier 缓存机制）
> - #6 (单元测试): 测试后期统一处理

---

## Commit 9: `64e1518cfd2` — [FLINK-39018][network] Fix LocalInputChannel priority event and buffer availability for recovered buffers

**Review 结论**: 需要修改

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [x] | [ ] | Critical | `64e1518cfd2` | `LocalInputChannel.java` | L510-L520 | `releaseAllResources()` 未回收 `toBeConsumedBuffers` 中的 buffer，导致内存泄漏 | 遍历 `toBeConsumedBuffers` 并逐个 `recycleBuffer()`，然后 `clear()` |
| [x] | [ ] | Major | `64e1518cfd2` | `LocalInputChannel.java` | L286-L289 | `checkState` 断言过于激进。`FullyFilledBuffer` 拆分场景下 `toBeConsumedBuffers` 也会非空，触发 priority event 时可能从 `subpartitionView` 拿到非 priority 数据，直接抛 `IllegalStateException` | 改为优雅处理（`if` 判断），或在注释中说明为什么断言安全 |
| [x] | [ ] | Major | `64e1518cfd2` | `LocalInputChannel.java` | L329-L334 | 最后一个 recovered buffer 消费完后，`nextDataType` 被硬编码为 `DATA_BUFFER`，但实际可能是 event 或 priority event | 使用更准确的方式获取实际 `nextDataType`，或在注释中说明原因 |
| [x] | [ ] | Minor | `64e1518cfd2` | `LocalInputChannel.java` | L282-L314 | `hasPendingPriorityEvent` 在 `subpartitionView.getNextBuffer()` 返回 null 时未重置，导致不必要的后续调用 | 在 `next == null` 时也重置 `hasPendingPriorityEvent = false` |
| [ ] | [ ] | Minor | `64e1518cfd2` | `LocalInputChannel.java` | L246-L272 | `peekNextBufferSubpartitionIdInternal()` 未考虑 `toBeConsumedBuffers` 中的 recovered buffers | 评估是否需要优先检查 `toBeConsumedBuffers` |
| [x] | [ ] | Minor | `64e1518cfd2` | `LocalInputChannel.java` | L165-L167 | 原有的 `checkState(toBeConsumedBuffers.isEmpty())` 防御性检查被移除 | 保留条件性 `checkState`：`toBeConsumedBuffers.isEmpty() \|\| isUnalignedDuringRecoveryEnabled` |
| [ ] | [ ] | Minor | `64e1518cfd2` | `LocalInputChannel.java` | L441-L446 | `notifyPriorityEvent` 无条件设置 `hasPendingPriorityEvent = true`，非 recovery 路径下语义不干净 | 考虑仅在 `!toBeConsumedBuffers.isEmpty()` 时设置标志 |
| [ ] | [ ] | Minor | `64e1518cfd2` | 整体 | N/A | 缺少单元测试：priority event 处理、recovered buffer 可用性修正等关键逻辑无测试覆盖 | 添加测试覆盖上述场景 |

> **不采纳理由**:
> - #5 (`peekNextBufferSubpartitionIdInternal`): 该方法仅在 tiered storage 场景下使用，而 recovered buffer 在消费完毕后才会进入 tiered storage 消费路径，实际不会出现问题
> - #7 (`notifyPriorityEvent` 无条件设置标志): 不影响功能——当 `toBeConsumedBuffers` 为空时，`getNextBuffer` 直接走 `subpartitionView` 路径，`hasPendingPriorityEvent` 标志会在下次 `getNextBuffer` 中被正确处理
> - #8 (单元测试): 测试后期统一处理

---

## Commit 10: `36ab9a1fc6f` — [FLINK-38543][network] Buffer migration from RecoveredInputChannel to physical channels

**Review 结论**: 需要修改

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [x] | [ ] | Critical | `36ab9a1fc6f` | `LocalInputChannel.java` | L510-L520 | `releaseAllResources()` 中 `toBeConsumedBuffers` 的迁移 buffer 在 channel release 时不会被 recycle，导致 buffer 泄漏 | 在 `releaseAllResources()` 中遍历并 recycle 每个 buffer，然后 `clear()` |
| [x] | [ ] | Major | `36ab9a1fc6f` | `RemoteInputChannel.java` | L261-L264, L278-L281 | `checkPartitionRequestQueueInitialized()` 被替换为 `checkError()`，非 recovery 场景下降低防护能力 | 仅在 `receivedBuffers` 非空时跳过 client 初始化检查，为空时仍执行原有检查 |
| [x] | [ ] | Major | `36ab9a1fc6f` | `SingleInputGate.java` | L400-L424 | 嵌套锁 `inputChannelsWithData` -> `receivedBuffers` 与 `onRecoveredStateBuffer` 中反向锁顺序存在潜在死锁风险 | 将 buffer 提取移到 `synchronized (inputChannelsWithData)` 之外，或明确文档化锁顺序约束 |
| [ ] | [ ] | Minor | `36ab9a1fc6f` | `RecoveredInputChannel.java` | L140-L143 | `receivedBuffers.isEmpty()` 的 post-condition 检查未持有 `receivedBuffers` 锁 | 删除该检查或放入 `synchronized (receivedBuffers)` 块 |
| [x] | [ ] | Minor | `36ab9a1fc6f` | `RemoteInputChannel.java` | L168-L171 | `subpartitionId` 硬编码为 0，可能在多子分区场景下不正确 | 验证多子分区场景正确性，或从 `consumedSubpartitionIndexSet` 获取实际值 |
| [ ] | [ ] | Suggestion | `36ab9a1fc6f` | `RemoteInputChannel.java` | L162-L179 | buffer 迁移未在 `synchronized (receivedBuffers)` 中执行 | 为代码一致性，包裹在 `synchronized (receivedBuffers)` 中 |

> **不采纳理由**:
> - #4 (`receivedBuffers.isEmpty()` 无锁): 此处在 `toInputChannel()` 调用后执行，`clear()` 已完成且无并发写入，`checkState` 纯粹是逻辑断言而非并发安全需求
> - #6 (迁移未 synchronized): 构造函数中无并发访问，添加 synchronized 纯属形式主义

---

## Commit 11: `269436d4c54` — [FLINK-38543][checkpoint] Fix Mailbox loop interrupted before recovery finished

**Review 结论**: 通过

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [ ] | [ ] | Suggestion | `269436d4c54` | `StreamTask.java` | L935 | `thenRun` 返回的 future 被丢弃，如果 `suspend()` 抛异常会被静默吞掉 | 考虑添加 `.exceptionally()` 记录异常日志 |
| [ ] | [ ] | Suggestion | `269436d4c54` | `StreamTask.java` | L929-L932 | 注释中 "runs on the async thread" 表述不够准确 | 改为 "runs on the future's completion thread" |
| [ ] | [ ] | Suggestion | `269436d4c54` | `StreamTask.java` | N/A | 缺少针对此竞态条件修复的回归测试 | 考虑增加单元测试 |

> **不采纳理由**:
> - #1: `suspend()` 是简单的 poison mail 投递操作，几乎不可能抛异常，且即使异常也不影响 task 正确性（task 已在退出路径上）
> - #2: "async thread" 在上下文中含义清晰（区别于 mailbox thread），表述足够准确
> - #3: 竞态条件测试本身不稳定，很难可靠复现，投入产出比低

---

## Commit 12: `3096a39153e` — [FLINK-38543][checkpoint] Introduce bufferFilteringCompleteFuture for earlier RUNNING state transition

**Review 结论**: 需要修改

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [x] | [ ] | Critical | `3096a39153e` | `RecoveredInputChannel.java` | L311-L327 | `releaseAllResources()` 未处理 `bufferFilteringCompleteFuture`。task 取消时该 future 永远不会 complete，**task 可能挂起** | 在 `releaseAllResources()` 中对 `bufferFilteringCompleteFuture` 调用 `completeExceptionally()` 或 `complete(null)` |
| [x] | [ ] | Major | `3096a39153e` | `RecoveredInputChannel.java` | L207-L218 | `stateConsumedFuture` 完成时机问题：配置开启时 channel 被转换后 `getNextRecoveredStateBuffer()` 不再被调用，`stateConsumedFuture` 可能永远无法完成 | 需确认后续 commit 是否处理了此问题 |
| [x] | [ ] | Minor | `3096a39153e` | `RecoveredInputChannel.java` | L65-L70 | `bufferFilteringCompleteFuture` 的 Javadoc 说 "completes before stateConsumedFuture"，仅在配置开启时成立 | 修改注释明确前提条件 |
| [x] | [ ] | Minor | `3096a39153e` | `InputGate.java` | L195-L200 | `getBufferFilteringCompleteFuture()` 的 Javadoc 同样未说明前提条件 | 补充 "When disabled, this future may never complete" |
| [ ] | [ ] | Suggestion | `3096a39153e` | `IndexedInputGate.java` | L78-L88 | `setUnalignedDuringRecoveryEnabled` 声明为 `abstract`，导致所有 Mock 类需添加空实现 | 改为非 abstract 并提供空默认实现 |
| [ ] | [ ] | Suggestion | `3096a39153e` | `RecoveredInputChannelTest.java` | L185 | `AssertionError("channel conversion succeeded")` 消息具有误导性 | 改为描述性消息 |
| [ ] | [ ] | Suggestion | `3096a39153e` | `SingleInputGateTest.java` | L145-L175 | 缺少配置关闭时 gate 层面聚合 future 行为的对照测试 | 补充 `isUnalignedDuringRecoveryEnabled=false` 时的测试 |

> **不采纳理由**:
> - #5 (`abstract` vs 默认实现): `abstract` 强制所有实现者做出显式决定，是合理的 API 设计选择；Mock 类中添加空实现的成本极低
> - #6 (错误消息措辞): 测试代码中的错误消息，影响极低，不值得单独修改
> - #7 (对照测试): 测试后期统一处理

---

## Commit 13: `812481f112d` — [FLINK-38543][checkpoint] Change overall UC restore process for checkpoint during recovery

**Review 结论**: 需要修改（若干低严重性问题）

| 是否采纳 | 已修复 | 重要等级 | commit 来源 | 文件 | 行号 | 问题描述 | 修改建议 |
|---------|-------|---------|------------|------|------|---------|---------|
| [x] | [ ] | Minor | `812481f112d` | `IndexedInputGate.java` | L83-L84 | Javadoc 声称 "The default implementation does nothing" 但方法是 `abstract` 的 | 修改 Javadoc 或改为非 abstract 提供空默认实现 |
| [x] | [ ] | Minor | `812481f112d` | `StreamTask.java` | L908-L909 | 原始代码中关于增量 checkpointing 和 `FULL_CHECKPOINT` 的重要警告注释被完全删除 | 在新代码中保留相关警告注释 |
| [x] | [ ] | Minor | `812481f112d` | `RecoveredInputChannelTest.java` | L209-L224 | `TestableRecoveredInputChannel` 中 `inputGate` 字段遮蔽了父类同名字段（field shadowing） | 移除子类中冗余的 `inputGate` 字段声明 |
| [ ] | [ ] | Suggestion | `812481f112d` | `RecoveredInputChannelTest.java` | L147-L165 | 手动 for 循环测试两种配置，失败时无法区分哪种配置导致 | 使用 `@ParameterizedTest` + `@ValueSource` |
| [ ] | [ ] | Suggestion | `812481f112d` | `SingleInputGateTest.java` | L145-L175 | 缺少 config 关闭时 gate 级别 future 不完成的测试场景 | 补充对照测试 |
| [ ] | [ ] | Suggestion | `812481f112d` | `RecoveredInputChannelTest.java` | L185 | `AssertionError("channel conversion succeeded")` 消息容易引起困惑 | 改为描述性消息 |

> **不采纳理由**:
> - #4 (`@ParameterizedTest`): for 循环方式虽不如 `@ParameterizedTest` 优雅但功能等价，且改动仅涉及测试代码结构偏好
> - #5: 已在 commit 12 中采纳了相同建议（#7），不需要重复
> - #6: 同 commit 12 的 #6，测试代码中的错误消息，影响极低

---

## 统计汇总

| 等级 | 总数 | 建议采纳 | 不采纳 | 已修复 |
|------|------|---------|--------|--------|
| Critical | 7 | 7 | 0 | 0 |
| Major | 12 | 10 | 2 | 0 |
| Minor | 30 | 21 | 9 | 0 |
| Suggestion | 16 | 3 | 13 | 0 |
| **合计** | **65** | **41** | **24** | **0** |

> 不采纳中 6 项为"测试后期统一处理"

---

**各 commit 独立 review 报告**: `review_reports/commit_*.md`
**按严重等级汇总报告**: `review_reports/SUMMARY.md`
