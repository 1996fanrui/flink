# 验收方案：Phase 2 — InputChannel 侧

> Phase 2 完整重塑 task thread 一侧的 recovery 数据接收 / 消费 / checkpoint 持久化 / `stateConsumedFuture` 完成路径；所有断言均通过 `LocalInputChannelTest` / `RemoteInputChannelTest` 单元测试套覆盖。本 phase 不引入 ITCase。

## 状态表

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-32IM | `LocalInputChannel` / `RemoteInputChannel` 实现 `RecoverableInputChannel`：`onRecoveredStateBuffer` 投递 + `finishReadRecoveredState` 翻转 flag + `notifyChannelNonEmpty` 在队列由空变非空时触发 | REQ-Y6OP, REQ-D62D, REQ-GVDT | 通过 | 代码自动化 | LocalInputChannelTest + RemoteInputChannelTest 8/8 PASS（prior session） |
| AT-LA74 | `getNextBuffer()` 的 `inRecovery` 4 种边界（flag×queue 各组合）行为正确：阻塞普通 upstream、消费 recovery 数据、退出 recovery、退回 master 路径 | REQ-IQDA, REQ-Y6OP | 通过 | 代码自动化 | 11 个方法全部 PASS（prior session） |
| AT-V7CW | `checkpointStarted` 在 in-recovery 分支按 cpId-bounded 扫描 `recoveredBuffers`：捕获 `RecoveryCheckpointBarrier(cpId)` 后停止、pre-barrier buffer 全部 `retainBuffer()` 后交给 `addInputData`、sentinel 自身被队列移除 | REQ-G319 | 通过 | 代码自动化 | Local + Remote 各 4 项，共 8/8 PASS（prior session） |
| AT-LX1M | `checkpointStarted` 在非 in-recovery 分支保留 master 既有行为（Remote 走 `channelStatePersister.startPersisting + maybePersist`；Local 走 `startPersisting(barrier.getId(), Collections.emptyList())`）；in-recovery 时 `receivedBuffersHasNoLiveDataBuffer()` 断言守护通过/不通过案例覆盖 | REQ-G319 | 通过 | 代码自动化 | 5/5 PASS（prior session） |
| AT-W4N5 | `stateConsumedFuture` 完成路径两种触发顺序：(a) `finishReadRecoveredState` 时 `recoveredBuffers` 已为空、(b) 标志已 true、`getNextBuffer` 弹出最后一项；均完成唯一一次 | REQ-RYGK | 通过 | 代码自动化 | Local + Remote 各 3 项，共 6/6 PASS（prior session） |
| AT-AEVL | `RecoveredInputChannel.toInputChannel()` migration 改走 `onRecoveredStateBuffer` + `finishReadRecoveredState`：顺序投递、构造器不再接 `initialRecoveredBuffers`、迁移完成后下游 channel 可正常被消费 | REQ-Y4RX, REQ-TWEE, REQ-YW7I | 通过 | 代码自动化 | 3/3 PASS（prior session） |
| AT-OOXP | FLINK-39018 系列 9 个回归测试按新形态（构造 channel → `onRecoveredStateBuffer` 顺序投递 → `finishReadRecoveredState`）改写后继续通过；FullyFilledBuffer 拆分热路径无 recovery-aware 逻辑残留 | REQ-Y6OP, REQ-D62D, REQ-IQDA, REQ-TWEE, REQ-YW7I | 通过 | 代码自动化 | LocalInputChannelTest 9/9 PASS（prior session） |

---

## 验收步骤

### [L1-测试] AT-32IM `RecoverableInputChannel` 接口实现

**目的**：验证 `LocalInputChannel` 与 `RemoteInputChannel` 的 `onRecoveredStateBuffer` / `finishReadRecoveredState` 行为符合 `simplify_approach/overview.md` §6.2：

- `onRecoveredStateBuffer` 在 `recoveredBuffers` 由空变非空时调用 `notifyChannelNonEmpty()` 一次
- 已 release 的 channel 投递 buffer 被静默 `recycleBuffer()`
- `finishReadRecoveredState` 翻转 `allRecoveredBuffersDelivered` 标志为 true（不可重入翻第二次）
- 锁：Remote 用 `synchronized(receivedBuffers)`，Local 用 `synchronized(recoveredBuffers)`

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='LocalInputChannelTest#testOnRecoveredStateBufferEnqueues+testOnRecoveredStateBufferOnReleasedChannelIsSilentlyRecycled+testFinishReadRecoveredStateFlipsFlagOnce+testOnRecoveredStateBufferNotifiesChannelNonEmptyOnEmptyToNonEmptyTransition,RemoteInputChannelTest#testOnRecoveredStateBufferEnqueues+testOnRecoveredStateBufferOnReleasedChannelIsSilentlyRecycled+testFinishReadRecoveredStateFlipsFlagOnce+testOnRecoveredStateBufferNotifiesChannelNonEmptyOnEmptyToNonEmptyTransition'
```

**预期结果**：8 项测试全部 PASS。

---

### [L1-测试] AT-LA74 `getNextBuffer()` inRecovery 边界行为

**目的**：覆盖 `inRecovery` 4 个边界状态在 Local 与 Remote 上的行为：

| flag=false, queue=空 | inRecovery=true | `getNextBuffer` 返回 `Optional.empty()`（阻塞普通 upstream） |
| flag=false, queue=非空 | inRecovery=true | `getNextBuffer` 弹出 `recoveredBuffers` 队首 |
| flag=true, queue=非空 | inRecovery=true | 同上，弹出最后一项时同步触发 `stateConsumedFuture` |
| flag=true, queue=空 | inRecovery=false | `getNextBuffer` 走 master 既有路径（Local 走 `toBeConsumedBuffers` / `subpartitionView`；Remote 走 `receivedBuffers`） |

同时覆盖 `hasPendingPriorityEvent`（Local）/ `addPriorityBuffer`（Remote）的优先事件分支。

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='LocalInputChannelTest#testInRecoveryBoundaryFlagFalseQueueEmptyReturnsEmpty+testInRecoveryBoundaryFlagFalseQueueNonEmptyPolls+testInRecoveryBoundaryFlagTrueQueueNonEmptyPolls+testInRecoveryBoundaryFlagTrueQueueEmptyFallsToMasterPath+testPriorityEventDuringRecoveryFetchedFromSubpartitionView+testPriorityEventDuringRecoveryResetAfterNonPriority,RemoteInputChannelTest#testInRecoveryBoundaryFlagFalseQueueEmptyReturnsEmpty+testInRecoveryBoundaryFlagFalseQueueNonEmptyPolls+testInRecoveryBoundaryFlagTrueQueueNonEmptyPolls+testInRecoveryBoundaryFlagTrueQueueEmptyFallsToMasterPath+testPriorityEventDuringRecoveryViaAddPriorityBuffer'
```

**预期结果**：上述 11 个测试方法全部 PASS（4 个 inRecovery 边界 × 2 channel + Local 2 个 priority 用例 + Remote 1 个 priority 用例）。开发阶段必须按这些方法名实现单测，禁止用通配符或 "to be determined" 模糊化。

---

### [L1-测试] AT-V7CW `checkpointStarted` in-recovery 分支 cpId-bounded 扫描

**目的**：模拟 task thread Step 1 已向 channel 投递 `RecoveryCheckpointBarrier(cpId)` sentinel 的场景，验证 `checkpointStarted` in-recovery 分支：

- 扫描到 sentinel 时停止，且 sentinel 从队列移除
- pre-barrier 的所有 buffer `retainBuffer()` 后通过 `channelStateWriter.addInputData(...)` 提交
- post-barrier 的元素保留在队列内，供后续消费 / 后续 cpId 持久化使用
- 多 cpId 嵌套场景：队列内同时存在 `RecoveryCheckpointBarrier(cp1)` 与 `RecoveryCheckpointBarrier(cp2)` 时，`checkpointStarted(cp1)` 只持久化到 cp1 sentinel，cp2 sentinel 与其后 buffer 不被消费

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='LocalInputChannelTest#testCheckpointStartedScansRecoveredBuffersUpToBarrier+testCheckpointStartedRetainsPreBarrierBuffers+testCheckpointStartedRemovesSentinel+testCheckpointStartedNestedCpIds,RemoteInputChannelTest#testCheckpointStartedScansRecoveredBuffersUpToBarrier+testCheckpointStartedRetainsPreBarrierBuffers+testCheckpointStartedRemovesSentinel+testCheckpointStartedNestedCpIds'
```

**预期结果**：8 项测试全部 PASS。

---

### [L1-测试] AT-LX1M `checkpointStarted` 双分支互斥

**目的**：

- 非 in-recovery 时进入 master 既有分支：Remote 调用 `channelStatePersister.startPersisting` 后续 `maybePersist` 链路；Local 调用 `startPersisting(barrier.getId(), Collections.emptyList())`
- in-recovery 阶段 `receivedBuffersHasNoLiveDataBuffer()` 断言：Remote 在 `receivedBuffers` 含 priority/control buffer 时通过；含 live data buffer（`buf.isBuffer() == true`）时 assert 触发；Local 实现恒为 true（无 receivedBuffers 字段）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='LocalInputChannelTest#testCheckpointStartedNotInRecoveryUsesMasterPath+testReceivedBuffersHasNoLiveDataBufferIsTrueOnLocal,RemoteInputChannelTest#testCheckpointStartedNotInRecoveryUsesMasterPath+testReceivedBuffersHasNoLiveDataBufferDetectsLiveData+testReceivedBuffersHasNoLiveDataBufferAcceptsPriorityOnly'
```

**预期结果**：5 项测试全部 PASS。

---

### [L1-测试] AT-W4N5 `stateConsumedFuture` 完成路径

**目的**：覆盖两种触发顺序：

- (a) `recoveredBuffers` 已空 → `finishReadRecoveredState()` 翻转 flag → future complete
- (b) flag 已 true → `getNextBuffer()` 弹出最后一项 → future complete

并验证 future 仅 complete 一次（多次满足条件时 `complete()` 不抛 `IllegalStateException` 或重复回调）。

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='LocalInputChannelTest#testStateConsumedFutureCompletesOnFinishReadRecoveredStateWhenQueueEmpty+testStateConsumedFutureCompletesOnLastConsumeWhenFlagTrue+testStateConsumedFutureCompletesOnce,RemoteInputChannelTest#testStateConsumedFutureCompletesOnFinishReadRecoveredStateWhenQueueEmpty+testStateConsumedFutureCompletesOnLastConsumeWhenFlagTrue+testStateConsumedFutureCompletesOnce'
```

**预期结果**：6 项测试全部 PASS。

---

### [L1-测试] AT-AEVL `RecoveredInputChannel.toInputChannel()` migration 改走新接口

**目的**：

- `RecoveredInputChannel.toInputChannel()` 调用顺序：构造下游 channel（构造器不再接 `initialRecoveredBuffers`）→ 对剩余 buffer 顺序 `onRecoveredStateBuffer(buf)` → 调用 `finishReadRecoveredState()`
- 迁移完成后 `getNextBuffer()` 可正常按 `recoveredBuffers` 弹出元素
- `RecoveredInputChannel` 子类 `toInputChannelInternal()` 签名删除 `ArrayDeque<Buffer> remainingBuffers` 参数
- 缺失实现 `RecoverableInputChannel` 的 channel 类型时 `toInputChannel()` 抛 `IllegalStateException`（防御性测试）

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='LocalRecoveredInputChannelTest#testToInputChannelUsesOnRecoveredStateBufferAndFinishReadRecoveredState,RemoteRecoveredInputChannelTest#testToInputChannelUsesOnRecoveredStateBufferAndFinishReadRecoveredState,RecoveredInputChannelTest#testToInputChannelMigrationOrder'
```

**预期结果**：3 项测试全部 PASS。

---

### [L1-测试] AT-OOXP FLINK-39018 9 个回归测试按新形态改写后通过

**目的**：FLINK-39018 + 准备阶段共 9 个测试（清单同 `decouple_toBeConsumedBuffers/acceptance_test.md`）必须按新形态改写为"构造 channel → `onRecoveredStateBuffer` 顺序投递 → `finishReadRecoveredState`"，改写后继续覆盖以下既有语义：

- recovery 数据消费顺序
- 优先事件穿插
- 最后一条 recovered buffer 的 next data type 动态探测
- checkpoint inflight 持久化范围

并且 FullyFilledBuffer 拆分热路径已彻底脱离 recovery-aware 逻辑。

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='LocalInputChannelTest#testCheckpointStartedPersistsRecoveredBuffers+testPriorityEventConsumedBeforeRecoveredBuffers+testPriorityEventFailsFastWhenSubpartitionViewIsNull+testPriorityEventFailsFastWhenNonPriorityBufferReturned+testPriorityEventFailsFastWhenSubpartitionViewReturnsNull+testMultipleConsecutivePriorityEvents+testNextDataTypeCorrectedToRecoveredBufferType+testGetBuffersInUseCountIncludesToBeConsumedBuffers+testGetNextBufferWithMigratedRecoveredBuffers'
```

**预期结果**：9 项测试全部 PASS。

---

## 备注

- 验收 L1 步骤必须由 `flink-test-runner` sub agent 执行
- 测试用例命名必须严格匹配本文档列出的方法名；开发阶段不允许使用通配符 / 模糊命名 / "to be determined" 之类的占位
- 若 mvn 报 unresolved symbol，先执行 `./mvnw clean install -U -Pfast -DskipTests -P java11-target -P java11` 重新编译再重试
- 本 phase 不引入 ITCase；端到端 UC during recovery / OOM 修复验证留给 Phase 5
