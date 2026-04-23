# FLINK-38544 Stale Migration Code & RecoveredStore Refactor 讨论

本文档汇总三条互相关联的议题：

1. 由 FLINK-39018 引入、但在当前 store 架构下已经失效的逻辑（死代码清理）。
2. `ChannelStatePersister.startPersisting` 接收 `RecoveredBufferStore` 的集中化重构。
3. Checkpoint 对 OutputWriter 内未投递数据（活跃 buffer / 活跃 spill entry / FIFO spill 队列）的处理，
   目前完全未实现。

讨论过程跨多轮，重要结论汇总在本文档，避免下一次开工时细节丢失。

---

## 0. Fix 规范（可复用）

> 基于 cherry-pick 原始变更 + 补 fix 的开发流程通用规范。其他 agent / 开发者接手同类任务时
> 必须遵守。所有文档引用统一使用 JIRA ID（不出现 git hash 或序号）。

**核心原则**：每个原始 JIRA（记为 JIRA i）对应的变更后**紧跟 ≤1 个** fix 变更，该 fix 变更捆绑
针对 JIRA i 的**所有**修改（可跨多个 feature / 修复点）。

**约束**：
1. 一个 fix 变更**只绑定一个** JIRA——跨 JIRA i / JIRA j 的改动必须按"主要归属"拆到对应 fix。
2. 一个 JIRA **只能有一个** fix 变更——多点 fix 合并其中，以多段 diff 出现。
3. 顺序严格 `... JIRA i → JIRA i 的 fix → JIRA i+1 → JIRA i+1 的 fix → ...`，禁止跨越。
4. 原始 JIRA 无需修改的不加 fix，直接进入下一 JIRA。

**目的**：
- **fix 必须作为独立变更保留**，不要自动 squash。人工 review 时能够单独查看每个 fix 的 diff
  （相对原始变更是纯增量），review 通过后由人工决定是否手动 squash。
- 避免跨 JIRA 依赖导致编译断层或 rebase 冲突；每对 (JIRA i + JIRA i 的 fix) 独立可验证。
- 最终（由人工决定）squash (JIRA i + JIRA i 的 fix) 可得"完整自洽"的单个干净变更，用于正式合入。

**跨 JIRA 依赖处理**：
- 若 JIRA i 的 fix 需要引用 JIRA j（j 在 i 之后）引入的新 API，把该改动**挪到 JIRA j 的 fix**（作为
  JIRA j 新 API 的首次使用者），不塞进 JIRA i 的 fix。例：phase2 流式写入本想修 FLINK-39521 的
  OutputWriter，但需 FLINK-39523 的流式 overload，故归入 FLINK-39523 的 fix。
- 若 JIRA i 的 fix 依赖 JIRA j 的 fix（j 在 i 之后）的符号，同理挪到后者；或在 JIRA i 的 fix 提前
  只声明不使用，JIRA j 的 fix 再使用。

---

## 1. 背景：旧迁移路径 vs. 新 store 路径

### 旧路径（FLINK-39018 的 4 项变更）
- `RecoveredInputChannel.toInputChannel()` 通过 `ArrayDeque<Buffer> initialRecoveredBuffers` 把 recovered
  buffers 传给新 physical channel 构造器。
- `LocalInputChannel` 塞进 `toBeConsumedBuffers`（`BufferAndBacklog` 包装）。
- `RemoteInputChannel` 塞进 `receivedBuffers`（`SequenceBuffer` 包装）。
- 由于 recovered 数据和 "normal" 数据共用同一 deque，下游所有读/计数/快照/优先事件/释放逻辑必须兼顾
  两种语义。FLINK-39018 随后的几项 patch 都是在这个共用 deque 的消费/快照路径上打的补丁。

### 新路径（FLINK-39522, FLINK-38544）
- recovered 数据住在 per-channel `RecoveredBufferStore`，通过 store reference 传递，不再走 deque。
- `toBeConsumedBuffers` 只剩 `FullyFilledBuffer` split（正常数据路径）。
- `receivedBuffers` 只剩网络数据。

### 关键后果
所有"为了让旧 deque 同时承载 recovered 语义"而打的补丁，**根因已不存在**。若不主动清理，就变成
误导读者的死代码 / 不再有语义的 side effect。

---

## 2. 架构决策（已达成共识）

以下均已与用户明确确认。

### 2.1 组件粒度

| 组件 | 粒度 | 代码佐证 |
|------|------|---------|
| `OutputWriter` | per-task（一个 task 的全部 gate / channel 共用） | `SequentialChannelStateReaderImpl:83` 创建唯一 instance，传入所有 `inputGates` |
| `RecoveredBufferStore` | per-channel | `createPerChannelStores(inputGates)` 产出 `Map<InputChannelInfo, Store>` |
| Spill file | per-task（与 OutputWriter 同生命周期） | REQ-SFMG |
| `spillEntryQueue` | per-task 全局 FIFO（不同 channel entry 交错） | `OutputWriterImpl:74` |

**不对称**：OutputWriter 是 per-task，store 是 per-channel。这是 disk 数据 checkpoint 需要聚合触发的根因。

### 2.2 数据路径

- `filterAndRewrite` → `OutputWriter.write(data, length, channelInfo)`。
- OutputWriter 根据 `channelInfo` 把数据直接投递到目标 store（buffer 走 `addBuffer`，磁盘走 spill file 并
  递增 store 的 `pendingCount`）。
- InputChannel 只认 store，通过 `store.tryTake()` 消费，**不直接感知 OutputWriter**。
- Store **只持有** ready buffers 队列 + `pendingCount`（磁盘未消费 entry 数）。不持有 SpillEntry 对象、不持有
  SpillFile reader、不做磁盘 I/O。

### 2.3 Checkpoint 路径（关键回调链）

```
InputChannel.checkpointStarted(barrier)
  └─ channelStatePersister.startPersisting(barrier.getId(), store, inflightBuffers)
       ├─ store.checkpoint(channelStateWriter, id, channelInfo)
       │    ├─ snapshot ready buffers: addInputData(CloseableIterator<Buffer>)
       │    └─ 回调 OutputWriter: checkpointCallback.onChannelCheckpointStarted(id, channelInfo)
       │
       └─ channelStateWriter.addInputData(id, channelInfo, UNKNOWN, inflightBuffers)
            （Remote 传 getInflightBuffersUnsafe；Local 传 emptyList）

OutputWriter.onChannelCheckpointStarted(id, channelInfo):
  ├─ 首个 callback 到达时：扫描 spillEntryQueue，计算 wait-set = {有 pending entry 的 channel}
  ├─ 从 wait-set 移除 channelInfo
  └─ wait-set 空 → 触发"磁盘数据阶段 snapshot"
        顺序遍历 spillEntryQueue：
          for entry in spillEntryQueue:
            writer.addInputData(id, entry.channelInfo, seqNum,
                                spillReader.openInputStream(entry.offset, entry.length),
                                entry.length)
```

要点：
- **InputChannel 不直接调 `store.checkpoint`**；经过 `ChannelStatePersister.startPersisting` 统一入口。
- **Store 不直接调 OutputWriter**：store 通过构造时注入的 `checkpointCallback` 通知（接口解耦）。
- **OutputWriter 不感知 InputChannel**，只持有 `storesByChannel` 和每个 store 的 callback。
- 磁盘数据的顺序写全发生在最后一个 channel callback 到达后，**一次顺序 I/O**，没有随机 I/O。

### 2.4 单 checkpoint 语义

参考 `ChannelStatePersister.startPersisting`：**同一时刻只有一个活跃 checkpoint**。新 barrierId 到达意味着
旧的已结束或放弃。OutputWriter 的 wait-set 状态随之覆盖，不需要按 `checkpointId` 维护 Map。

### 2.5 Checkpoint 触发时无 race

Checkpoint 触发点在 Task 线程；Recovery 线程要么在 filterAndRewrite 循环的 `write()` 调用之间、要么在
`close()` drain 循环中。**触发时机保证所有 buffer 已刷出**——不会在 `write()` 调用中途触发，因此
活跃 buffer / 活跃 SpillEntry（用户确认了这一点）不会在 snapshot 时还"半填"。Snapshot 直接对
稳态的 spillEntryQueue + store.readyBuffers 做即可。

> **名词**：`seal` 指把 OutputWriter 正在累积的 SpillEntry（还未满 memorySegmentSize，或 channel 还未变）
> 固化成不可变的 SpillEntry 对象并加入全局 FIFO 队列。来自 `design.md` 的"密封"描述。

### 2.6 disk + network inflight 不共存不变量

**约定**：单 channel 内，`store.pendingCount > 0`（磁盘未消费）时，`receivedBuffers` 里的 data buffer
必须为空；反过来，`receivedBuffers` 出现 DATA_BUFFER 时，store 必须已 drain 完成。二者不共存——
否则 checkpoint snapshot 顺序会乱（`store.checkpoint()` 的 ready buffers + OutputWriter 异步 drain 的
spill 字节 + `ChannelStatePersister.startPersisting` 写入的 `knownBuffers` 三段会错位，恢复时无法还原）。

**enforcement 来源**：两条前提叠加天然保证，**无需额外 credit 机制**。

1. **上游在 filtering 模式下无 output state 可 replay**。前置配置 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true`
   （即 `CheckpointingOptions.isCheckpointingDuringRecoveryEnabled` 下的 filtering 路径必备条件），此时
   `StateAssignmentOperation.java:175-182` 会把上游的 output buffers 经 `distributeOutputBuffersToDownstream()`
   全量下发给下游，上游侧 output channel state 为空。上游 `PipelinedResultPartition.finishReadRecoveredState`
   只会 emit 一个 RECOVERY_COMPLETION event，**不发任何 DATA_BUFFER**。
2. **`RemoteInputChannel.getNextBuffer` store 优先 poll**（`RemoteInputChannel.java:282`）：store 非空时，
   `receivedBuffers` 里的任何东西（包括穿透 credit 的 event）都不会被取出，必然先 drain store。

**推导**：
- 上游不 replay → store 非空期间 `receivedBuffers` 里最多只有 RECOVERY_COMPLETION event，没有 DATA_BUFFER。
- RECOVERY_COMPLETION 是 event，被 `RemoteInputChannel.getInflightBuffersUnsafe:879-900` 的 `isBuffer()` 判断
  跳过，不会进 `knownBuffers`，不破坏 invariant。
- Fresh DATA_BUFFER 的发送链路是：下游 task poll 到 RECOVERY_COMPLETION → `UpstreamRecoveryTracker.handleEndOfRecovery`
  → 所有 channel 就绪 → `resumeConsumption` RPC → 上游解 `isBlocked`。由于 task poll 顺序 store 优先，
  必然等 store 完全 drain 之后才会 poll 到 RECOVERY_COMPLETION，所以 fresh DATA_BUFFER 进入
  `receivedBuffers` 的时刻**必然晚于** store 变空，二者不交错。

**已放弃的方案**：
- ~~延迟 `requestSubpartitions()`~~：会阻塞 checkpoint barrier 从上游流向下游。
- ~~副队列缓冲 `onBuffer`~~：破坏网络路径简洁性。
- ~~credit=0 gating + `releaseHeldCredit` + `onBecameEmpty` callback~~：在 filtering 模式下冗余。上游无
  replay data，credit gating 能拦住的 DATA_BUFFER 不会出现；event 绕 credit，gating 也拦不住但无害。
  gating 只会增加维护成本和出 bug 的面积。

**实现触点**：
- 依赖 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true`（filtering 模式强制前置，配置校验保证）。
- 依赖 `RemoteInputChannel.getNextBuffer` 先检查 `recoveredStore.isEmpty()` 再 poll `receivedBuffers`
  （已实现）。
- `ChannelStatePersister.startPersisting` 保留 `checkState(store.isEmpty() || knownBuffers.isEmpty())`
  作为**防御性兜底**：若未来误改 filter 配置或上游 state distribution 逻辑，能在 checkpoint 时刻 fail-fast。

### 2.7 EMPTY 单例

- `RecoveredBufferStore.EMPTY` 单例：`isEmpty()=true`, `isComplete()=true`, `tryTake()=null`, `size()=0`,
  `peekNextDataType()=NONE`, `checkpoint()`/`releaseAll()`/`setNotificationCallback()` 全 no-op。
- 构造器**无条件持有** store，删除 `!isEmpty()` 守卫（该守卫本身有 bug：转换瞬间 store 可能空，但后续
  OutputWriter 仍会 `addBuffer`；守卫住就会丢数据）。
- Non-filtering 路径传 EMPTY，filtering 路径传真实 store。
- 所有 `recoveredStore != null` 删除，只留 `!store.isEmpty()` / `!store.isComplete()`。
- `setNotificationCallback` 提升到接口，消 `instanceof RecoveredBufferStoreImpl`。
- 用户的性能顾虑：真实 store 消费完 `isEmpty()=true`，热路径行为与 EMPTY 等价，无额外开销。

### 2.8 Non-filtering 模式不受影响

OutputWriter 不存在，store = EMPTY，`store.checkpoint()` 是 no-op（不回调 OutputWriter）。
`ChannelStatePersister.startPersisting` 对 EMPTY store 表现与原来传 `emptyList` 相同。Non-filtering
路径不需要 callback 机制。

---

## 3. 代码审计表

每一行：设施、旧路径动机、当前 store 路径下的状态、建议处理。

### A. `LocalInputChannel.checkpointStarted` 遍历 `toBeConsumedBuffers`

**位置**：`LocalInputChannel.java:141-163`。

**旧动机**（FLINK-39018 LocalInputChannel checkpoint 快照分支）：recovered buffers 住在 `toBeConsumedBuffers` 里，要 snapshot 它们。

**当前事实**：
- Recovered 快照由 `store.checkpoint(...)` 处理（同方法内，行 142-152）。
- `toBeConsumedBuffers` 现在只含 `FullyFilledBuffer` 切出来的尾巴——**普通数据路径**的内存切片，
  不属于 channel state。
- Local channel 在 master 上原本就不 snapshot inflight（`startPersisting(id, Collections.emptyList())`），
  因为 Local 无网络 buffering，barrier 与数据同批到达。

**判断**：死代码。`FullyFilledBuffer` 切片不应进入 channel state，否则 UC 快照会多写一段本不该持久化的
普通数据，恢复时会有副作用（重复消费）。

**建议**：恢复为 `channelStatePersister.startPersisting(barrier.getId(), store, Collections.emptyList())`
（配合 §4.1）。

### B. `RemoteInputChannel.checkReadability()` 特判

**位置**：`RemoteInputChannel.java:940-947`；call sites `:260, :295`。

**旧动机**（FLINK-39018 Buffer migration 分支）：migrated recovered buffers 在 `receivedBuffers` 里可能在 `requestSubpartitions()`
前就被读到，所以不能强求 `partitionRequestClient` 已初始化。

**当前事实**：`receivedBuffers` 只含网络数据；`getNextBuffer` 已先从 store 出数据（`:275-284`），
`receivedBuffers` 那层永远是纯网络路径，必要求 client 初始化。

**判断**：死代码 hack。对应 `design_impl_alignment.md` C4-2 "待修复"。

**建议**：删除 `checkReadability()`，两处调用改回 `checkPartitionRequestQueueInitialized()`（master 原行为）。

### C. `RemoteInputChannel.checkpointStarted` 里的 `getInflightBuffersUnsafe`

**位置**：`RemoteInputChannel.java:732-764`。

**判断**：**不是**死代码。Remote 在 master 上就要 snapshot 网络 inflight；回到原语义后仍然正确。
列在这里是为了对比：Local（A）和 Remote（C）结论相反，不要一刀切。

### D. `RecoveredInputChannel.onRecoveredStateBuffer()`

**位置**：`RecoveredInputChannel.java`。

**旧动机**：OutputWriter 前时代，ChannelStateHandler 直接 callback 这个方法把 buffer 塞入 channel。

**当前事实**：OutputWriter 通过 `store.addBuffer()` 直接投递，按 design.md REQ-7388 应删除该包装方法。
对应 `design_impl_alignment.md` C4-1 "待修复"。

**建议**：删除 `onRecoveredStateBuffer()`，grep 并移除所有调用点。

### E. Local / Remote 构造器的 null 守卫 + `instanceof RecoveredBufferStoreImpl`

**位置**：
- `LocalInputChannel.java:128-134`
- `RemoteInputChannel.java:170-176`
- 下游 15 处 `recoveredStore != null` 散落调用（Local 8, Remote 7）

**问题**：
1. 15 处 null 判断。
2. `instanceof RecoveredBufferStoreImpl` 反射式强转调用 `setNotificationCallback`——接口上没这个方法。
3. 构造器里 `if (recoveredStore != null && !recoveredStore.isEmpty()) this.recoveredStore = recoveredStore`：
   转换瞬间 store 可能为空（drain 还没把 disk 数据加载回来），但随后 OutputWriter 会继续 `addBuffer`；
   以"当前为空"为由丢掉引用就会丢数据。

**建议**：采纳 §2.7 的 EMPTY 单例方案。

### F. `LocalInputChannel.getNextRecoveredBuffer` 的优先事件分支

**位置**：`LocalInputChannel.java:357-413`。

**旧动机**（FLINK-39018 LocalInputChannel 优先事件修复）：recovered 和 subpartitionView 数据共存时，UC barrier 先从 subpartitionView 取，
再回到 recovered。

**当前事实**：数据源已从 `toBeConsumedBuffers.peek()` 改为 `recoveredStore.peekNextDataType()`。
drain 期间 `subpartitionView` 已存在、barrier 仍可能先到，语义有效。

**判断**：保留。

### G. `LocalInputChannel.getBuffersInUseCount` / `unsynchronizedGetNumberOfQueuedBuffers`

**位置**：`LocalInputChannel.java:564-574`。

**旧动机**（FLINK-39018 `getBuffersInUseCount` hotfix）：加 `toBeConsumedBuffers.size()`，因为 recovered buffers 在里面要计入。

**当前事实**：`toBeConsumedBuffers` 只含 `FullyFilledBuffer` 切片——也是队列里的数据，**应当**被计数。
该 hotfix 实际修的是 master 漏计 FullyFilledBuffer split 的 bug（跟 recovered 无关）。

**判断**：保留。可能在整理阶段作为 master hotfix 独立发。

### H. `LocalInputChannel.releaseAllResources` 回收 `toBeConsumedBuffers`

**位置**：FLINK-39018 `getBuffersInUseCount` hotfix 引入。

**判断**：FullyFilledBuffer 切片也要释放，保留。

### I. Remote 构造器里的 `initialRecoveredBuffers → receivedBuffers` 迁移循环

**位置**：已在 FLINK-39522 中移除。确认无残留。

### J. OutputWriter 内未投递数据的 Checkpoint 处理

详见 §4.3。design.md 写了两阶段方案，代码**完全未实现**：
- `OutputWriterImpl` 没有任何 `checkpoint(...)` / `addInputData(InputStream)` / `openInputStream` 调用。
- 没有"等所有 channel 触发完"的聚合点。
- `ChannelStateWriter.addInputData(InputStream, dataLength)` 已存在但**无人调用**。
- `SpillFileReader.openInputStream` 已存在但无人调用。

**后果**：recovery 期间触发 checkpoint，spill 文件里未重放的数据会丢失。REQ-KM7C 未实现。

---

## 4. 关键重构方案

### 4.1 `ChannelStatePersister` 集中化

**新签名**：
```java
void startPersisting(long barrierId, RecoveredBufferStore store, List<Buffer> knownBuffers)
        throws CheckpointException;
```

**InputChannel 侧**（Local 示意，Remote 类似）：
```java
public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
    channelStatePersister.startPersisting(
            barrier.getId(), store, /* Local: emptyList; Remote: getInflightBuffersUnsafe */);
}
```

**ChannelStatePersister 内部**：
```java
void startPersisting(long barrierId, RecoveredBufferStore store, List<Buffer> knownBuffers) {
    // ... 原有 checkpointStatus / lastSeenBarrier 判定 ...

    // 防御性 invariant 检查：store 非空与 knownBuffers 非空互斥（由 §2.6 保证：
    // filtering 模式下上游无 output state replay + getNextBuffer store 优先 poll）
    checkState(
            store.isEmpty() || knownBuffers.isEmpty(),
            "Invariant violated: store has data (size=%s) AND knownBuffers non-empty (size=%s) at barrier %s. "
                    + "Requires UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true so upstream does not replay "
                    + "output state into receivedBuffers while the recovered store is still draining.",
            store.size(), knownBuffers.size(), barrierId);

    // 阶段 1-a: store ready buffers + 通知 OutputWriter
    try {
        store.checkpoint(channelStateWriter, barrierId, channelInfo);
    } catch (IOException e) {
        throw new CheckpointException(
                "Failed to checkpoint recovered store",
                CheckpointFailureReason.IO_EXCEPTION, e);
    }

    // 阶段 1-b: 已有 inflight 数据（Remote 网络 inflight）
    if (!knownBuffers.isEmpty()) {
        channelStateWriter.addInputData(barrierId, channelInfo,
                SEQUENCE_NUMBER_UNKNOWN,
                CloseableIterator.fromList(knownBuffers, Buffer::recycleBuffer));
    }
}
```

**防御性 check 说明**：invariant 要求 store 非空时 knownBuffers 必须为空（来自 §2.6：filtering 模式下
上游无 replay + store 优先 poll）。加一个 `checkState` 兜底，任何破坏 invariant 的改动（例如未来误改
filter 配置、`distributeOutputBuffersToDownstream` 行为变化、或 `getNextBuffer` 消费顺序被调整）会在
checkpoint 时立刻 fail，不会静默写出顺序错乱的 channel state。

**好处**：
- InputChannel 不直接调 `store.checkpoint`，单入口。
- try-catch IOException → CheckpointException 只写一次。
- Local 和 Remote 的 `checkpointStarted` 各缩成 1-2 行。

**前一轮讨论**：曾考虑"磁盘 snapshot 已由 OutputWriter 独立处理、startPersisting 加 store 没意义"，
被用户驳回——理由是 InputChannel 不应直接调 `store.checkpoint`。采纳集中化方案。

### 4.2 EMPTY 单例

见 §2.7。

### 4.3 OutputWriter disk checkpoint（J 节的完整方案）

**核心数据结构变化**：
- `OutputWriterImpl` 新增 `Map<InputChannelInfo, CheckpointCallback>`（每个 store 一份 callback 的反向注册，
  构造时建立）——或更简洁地让 OutputWriter 为每个 store `setCheckpointCallback(this::onChannelCheckpointStarted)`。
- `OutputWriterImpl` 新增状态：`long currentCheckpointId = -1`、`Set<InputChannelInfo> waitSet`。
- `RecoveredBufferStore` 接口新增 `setCheckpointCallback(CheckpointCallback)`——专用函数接口
  `@FunctionalInterface CheckpointCallback { void onChannelCheckpointStarted(long checkpointId, InputChannelInfo info); }`。
  不用 `BiConsumer<Long, InputChannelInfo>`，以明确语义并避免装箱。

**Store 侧**（`RecoveredBufferStoreImpl.checkpoint` 扩展）：
```java
synchronized void checkpoint(ChannelStateWriter writer, long id, InputChannelInfo info) {
    // 1) snapshot ready buffers（与现有一致）
    if (!readyBuffers.isEmpty()) {
        List<Buffer> retained = retainAll(readyBuffers);
        writer.addInputData(id, info, SEQUENCE_NUMBER_RESTORED,
                CloseableIterator.fromList(retained, Buffer::recycleBuffer));
    }
    // 2) 通知 OutputWriter
    if (checkpointCallback != null) {
        checkpointCallback.accept(id, info);
    }
}
```

**OutputWriter 侧**：
```java
synchronized void onChannelCheckpointStarted(long id, InputChannelInfo info) {
    if (id != currentCheckpointId) {
        // 新 checkpoint，重算 wait-set（单 checkpoint 语义）
        currentCheckpointId = id;
        waitSet = extractChannelsWithPending(spillEntryQueue); // 扫描一次
    }
    waitSet.remove(info);
    if (waitSet.isEmpty()) {
        drainSpillEntriesToCheckpoint(id);
    }
}

private void drainSpillEntriesToCheckpoint(long id) {
    // 顺序遍历，逐 entry 写入（ChannelStateWriter 按 channelInfo 聚合）
    for (SpillEntry entry : spillEntryQueue) {
        SpillFileReader reader = readerFor(entry);
        InputStream is = reader.openInputStream(entry.getOffset(), entry.getLength());
        channelStateWriter.addInputData(
                id, entry.getChannelInfo(),
                SEQUENCE_NUMBER_RESTORED, // 保持与 store.checkpoint ready buffers 一致
                is, entry.getLength());
    }
}
```

**要点**：
- Wait-set 在首个 callback 到达时扫描 `spillEntryQueue` 一次计算；之后 O(1) 移除。
- 没有 pending entry 的 channel 不在 wait-set 里；它的 callback 直接"空跑"，不影响流程。
- Callback 全部到位后，一次 `drainSpillEntriesToCheckpoint` 做顺序 I/O。
- `ChannelStateWriter.addInputData(InputStream)` 走流式重载，读 I/O 在 writer 的 executor 线程，**不在
  Task 线程上做磁盘读**。

**线程安全**：
- Store 内部已 synchronized；callback 在 store 的锁外调用（避免死锁）。
- OutputWriter 的 callback handler synchronized on `this`；`spillEntryQueue` / `currentCheckpointId` /
  `waitSet` 的访问在该锁下。
- Recovery 线程的 drain（close()）也要取同一把锁改 `spillEntryQueue`（现在没加，要补）。

### 4.4 切分策略：cherry-pick + 单 fix per JIRA

遵循 §0 Fix 规范：每个原始 JIRA 后紧跟 ≤1 个 fix，捆绑所有针对该 JIRA 的修改。

**两类变更**：
- **Base history（不 cherry-pick）**：FLINK-39018 的相关变更已在基线中。fix A/B/D 清理的就是它们
  留下的死代码，按"首个触及该文件的原始 JIRA"归入 FLINK-39522 的 fix。
- **6 个 FLINK-38544 原始 JIRA**：

| JIRA | 主题 |
|------|------|
| FLINK-39519 | Add source buffer heap allocation and buffer request interface |
| FLINK-39520 | Add SpillFile I/O components and RecoveredBufferStore |
| FLINK-39521 | Add OutputWriter with three data paths and drain loop |
| FLINK-39522 | Adapt InputChannels to consume from RecoveredBufferStore |
| FLINK-39523 | Add ChannelStateWriter streaming overload for disk data |
| FLINK-39524 | Integrate OutputWriter into filtering flow |

**每个 JIRA 的 fix 归属**：

| 原始 JIRA | fix 名称 | 内容 |
|-----------|---------|------|
| FLINK-39519 | — | 无需修正 |
| FLINK-39520 | **FLINK-39520 的 fix** | `CheckpointCallback` 接口 + `setCheckpointCallback` 加到接口+Impl；`setNotificationCallback` 升接口；`RecoveredBufferStoreImpl.checkpoint()` 内调用 callback（前文 J.2.a）；创建 `RecoveredBufferStore.EMPTY` 单例（所有方法 no-op） |
| FLINK-39521 | **FLINK-39521 的 fix** | OutputWriter：持有 callback 注册、wait-set 状态机（`onChannelCheckpointStarted` 入口、首次扫 `spillEntryQueue`、后续 O(1) 移除）、`synchronized(this)` 覆盖 queue/wait-set/checkpointId（drain 循环同锁）、构造时向各 store 注册 callback。**不含** phase2 磁盘写入（依赖 FLINK-39523） |
| FLINK-39522 | **FLINK-39522 的 fix** | §4.1 集中化（`startPersisting(barrierId, store, knownBuffers)` + `checkState(store.isEmpty() \|\| knownBuffers.isEmpty())` 作为防御性兜底）；fix A（Local 改 emptyList）；fix B（删 Remote.checkReadability）；fix D（删 RecoveredInputChannel.onRecoveredStateBuffer）；fix E（Local/Remote 无条件持有 store，默认 EMPTY，消 15 处 null 守卫 + instanceof） |
| FLINK-39523 | **FLINK-39523 的 fix** | OutputWriter `drainSpillEntriesToCheckpoint(id)`：遍历 queue 经 `SpillFileReader.openInputStream` 调 FLINK-39523 新增的 `ChannelStateWriter.addInputData(InputStream)`；"snapshot + drain" 合并避免双写 |
| FLINK-39524 | — | 无需修正 |

**最终 10 条变更**：

```
 1. FLINK-39519                                 6. FLINK-39522
 2. FLINK-39520                                 7. FLINK-39522 的 fix  ← §4.1 + A + B + D + E
 3. FLINK-39520 的 fix  ← Store 层完整            8. FLINK-39523
 4. FLINK-39521                                 9. FLINK-39523 的 fix  ← OutputWriter phase2 流式
 5. FLINK-39521 的 fix  ← OutputWriter 层       10. FLINK-39524
```

**fix 不自动 squash**，作为独立变更保留以便人工 review。10 条变更完整呈现给 reviewer；
review 通过后由人工决定是否手动 squash (原始 JIRA + 对应的 fix) 合成 6 条 clean 变更。

**Stage 映射**：
| Stage | 含 | 验证目标 |
|-------|---|---------|
| 1 | FLINK-39519 + FLINK-39520 + FLINK-39520 的 fix | Store 层自足，单测 Impl.checkpoint() 触发 callback |
| 2 | FLINK-39521 + FLINK-39521 的 fix | OutputWriter wait-set + 并发同步，单测状态机 |
| 3 | FLINK-39522 + FLINK-39522 的 fix | InputChannel 适配 + 集中化 + 死代码清理，Local/Remote 测试通过 |
| 4 | FLINK-39523 + FLINK-39523 的 fix + FLINK-39524 | disk checkpoint 流式写入贯通 + filtering 集成，端到端测试 |

### 4.5 实现细节清单

以下两条不需要额外设计拍板，但容易在 coding 时漏掉，列出作为 checklist。

#### 4.5.1 Phase 2 与 drain 的并发同步

Task 线程（`onChannelCheckpointStarted` 触发的 phase 2 遍历）和 Recovery 线程（`OutputWriterImpl.close()`
drain 循环）都访问/修改 `spillEntryQueue`。当前 `OutputWriterImpl` **完全没加同步**（`ArrayDeque` 非线程安全）。

实现要求：
- `OutputWriterImpl` 所有对 `spillEntryQueue` / `spillEntryReaderQueue` / `currentCheckpointId` / `waitSet`
  的访问 `synchronized(this)`。
- `drainSpillEntriesToCheckpoint(id)` 在锁内遍历并调 `addInputData(InputStream, length)`。
  注意：`addInputData` 本身是 enqueue 到 ChannelStateWriter 的 executor，不阻塞 I/O；在锁内调用安全。
- `close()` 的 drain 循环每次 `poll` 前拿锁；`onChannelCheckpointStarted` 也拿锁。两者互斥。
- 同一 SpillEntry 不会"既被 drain 消费又被 phase 2 snapshot"——锁保证原子性：
  - 若 drain 先拿锁：entry 从队列移除，投递 buffer 到 store；phase 2 后拿锁，只看到剩余 entry，缺失
    的那部分已在 store readyBuffers 里，由 `store.checkpoint()` 负责写出。
  - 若 phase 2 先拿锁：entry 还在队列，被 phase 2 写出；drain 后拿锁 poll 走同一 entry，投递到 store
    作为正常消费。此时 checkpoint 已写过，存在数据重复——需避免。

**避免重复的实现**：phase 2 写出的同时立刻把 entry 从 queue 里 poll 出来（"snapshot + drain" 合并）。
phase 2 完成后，队列为空；drain 线程后续 poll 只能拿到 phase 2 之后新加的 entry（正常路径下没有新的，
因为 filtering 已结束）。保证 entry 只被 snapshot 一次或被 drain 一次，不会双写。

#### 4.5.2 死代码 A 的改法与新签名一致

`LocalInputChannel.checkpointStarted` 的改法必须用 §4.1 的新签名：
```java
channelStatePersister.startPersisting(barrier.getId(), store, Collections.emptyList());
```
不要回退到旧签名 `startPersisting(barrierId, emptyList)`。否则 store 的 checkpoint 通路（ready buffers +
OutputWriter 回调）就断了。

Remote 同理：
```java
channelStatePersister.startPersisting(
        barrier.getId(), store, getInflightBuffersUnsafe(barrier.getId()));
```

---

## 5. Open Questions 与已定决议

### 已定决议

| # | 议题 | 决议 |
|---|------|------|
| Q1 | `disk + network inflight 不共存` 不变量 enforcement | **依赖 filtering 前置配置**：`UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true` 保证上游无 output state replay + `getNextBuffer` store 优先 poll 保证消费顺序。`ChannelStatePersister.startPersisting` 的 `checkState(store.isEmpty() \|\| knownBuffers.isEmpty())` 作为防御性兜底。详见 §2.6。 |
| Q2 | Wait-set 实现 | **首个 callback 到达时扫描 `spillEntryQueue` 一次**计算 wait-set，后续 O(1) 移除。 |
| Q3 | 磁盘数据 snapshot 的 `seqNum` | **保持与 ready buffers 一致**——`SEQUENCE_NUMBER_RESTORED`。本分支语义上只是把 buffer 搬到 disk 再回放，同一批 recovered 数据不应因存储介质不同而改 seqNum。 |
| Q4 | Callback 接口形态 | 专用 `@FunctionalInterface CheckpointCallback { onChannelCheckpointStarted(long, InputChannelInfo) }`，不用 `BiConsumer<Long, InputChannelInfo>`。 |
| Q5/Q6 | 切分与死代码清理 | 按 §0 Fix 规范：每个原始 JIRA 后最多 1 个 fix，捆绑所有针对该 JIRA 的改动。10 条变更（6 原始 JIRA + 4 fix）：FLINK-39519 → FLINK-39520 → FLINK-39520 的 fix → FLINK-39521 → FLINK-39521 的 fix → FLINK-39522 → FLINK-39522 的 fix → FLINK-39523 → FLINK-39523 的 fix → FLINK-39524。详见 §4.4。 |

### Q7（已定）. `ChannelStatePersister.startPersisting` 内的阶段顺序

签名 `startPersisting(barrierId, store, knownBuffers)` 内两段调用：
- `store.checkpoint(writer, id, channelInfo)`（内部 `addInputData(ready iterator)` + 回调 OutputWriter）
- `channelStateWriter.addInputData(id, channelInfo, UNKNOWN, knownBuffers iterator)`（Remote 网络 inflight）

**结论**：顺序无关，两段在任何 checkpoint 时刻**恰好互斥**，由 §2.6 invariant 保证
（filtering 模式上游无 replay + `getNextBuffer` store 优先 poll）。

- **Case A**：`!store.isEmpty()` → 上游无 replay data + fresh data 未发送（RECOVERY_COMPLETION
  尚未被下游 poll，`resumeConsumption` 未触发） → `receivedBuffers` 无 DATA_BUFFER →
  `knownBuffers = emptyList`。`startPersisting` 内只有 `store.checkpoint()` 有效
  （写 ready buffers + 回调 OutputWriter 触发 phase 2 写 disk）。
- **Case B**：`store.isEmpty()` → 下游 poll 到 RECOVERY_COMPLETION → `resumeConsumption` 发出 →
  上游可能开始发 fresh DATA_BUFFER → `receivedBuffers` 可能非空。
  `startPersisting` 内 `store.checkpoint()` 对 ready 是 no-op；回调进 OutputWriter 的 wait-set 时，
  该 channel 的 `pendingCount=0`，不在 wait-set 内（或 no-op 空跑）。
  `addInputData(knownBuffers)` 写网络 inflight。

**结论要点**：
- 两段不共存，任何顺序都正确。
- 实现上仍写成 `store.checkpoint()` 在前、`addInputData(knownBuffers)` 在后，语义直观。
- 不再依赖 `ChannelStateWriter` 的请求顺序保证（虽然 `ChannelStateWriterImpl` 确实是单线程 executor
  按 enqueue 顺序处理）。

---

## 6. 历次讨论记录

避免下次开工时细节丢失。

### Round 1：初始三问
- **Q1.1**：`ChannelStatePersister.startPersisting` 加 `RecoveredBufferStore` 参数。
  → 结论：采纳（§4.1）。曾一度被我建议放弃（理由：disk 独立处理后意义变小），用户驳回——
     InputChannel 不应直接调 `store.checkpoint`。
- **Q1.2**：Checkpoint 对 OutputWriter 未投递数据的处理。
  → 设计 gap，新方案见 §4.3。
- **Q1.3**：`recoveredStore` nullability（15 处 null 判断）。
  → 结论：采纳 EMPTY 单例（§2.7）。

### Round 2：旧实现 stale 审计
- 用户指出 `LocalInputChannel.checkpointStarted` 遍历 `toBeConsumedBuffers` 不合理（FullyFilledBuffer 切片
  不属于 channel state）。
- 扩展为系统 audit A-J（§3）。
- 用户要求：沿 FLINK-39018 引入、在 store 架构下已失效的逻辑统统清理，不要只看用户
  举的那一个例子。

### Round 3：OutputWriter 粒度
- 确认 per-task（§2.1 代码佐证）。
- 暴露 OutputWriter 与 store 的不对称性 → disk checkpoint 需要 per-task 聚合触发。

### Round 4：用户给出新架构
- OutputWriter 根据 `channelInfo` 直接投递到 store，不经 InputChannel。
- Store 持有数据（ready buffers）+ pending 计数，但**不**持有 spill file / reader。
- InputChannel 只消费 store。
- Checkpoint callback 机制：InputChannel → store → OutputWriter，中间不跳层。
- 最后一个 channel callback 到达时，OutputWriter 做**一次顺序遍历** `spillEntryQueue` 写入
  ChannelStateWriter，保证顺序 I/O。
- 只为"有 pending entry 的 channel"等 callback；无 pending 的 channel 不参与 wait-set。

### Round 5：进一步细节
- 用户澄清：callback 链是 `InputChannel → store → OutputWriter`，不是 `InputChannel → OutputWriter`。
- 用户澄清：`startPersisting` 接收 store 的方案仍然保留，InputChannel 不直接调 `store.checkpoint`。
- 用户提出 `disk + network inflight` 不共存不变量，要求代码强保证。
- `seal` 语义澄清（§2.5）。
- Checkpoint 触发时无 race：write() 不在中途触发，活跃 buffer/SpillEntry 不会"半填"。
- 单 checkpoint 语义：参考 `ChannelStatePersister`，无需并发 checkpoint 支持。
- Non-filtering 模式：EMPTY store，无需 callback 机制。
- 切分策略：cherry-pick based incremental fix（§4.4）。

### Round 6：§4.4 JIRA 映射迭代（开发前）
- 原表错把 FLINK-39018 的 4 项变更列为 cherry-pick 目标；实际在 base 历史里不动。漏了
  FLINK-39519 / FLINK-39520。改为明确区分 base history 与待 cherry-pick 的 6 个 FLINK-38544 JIRA。
- 初版为 fix J 把 FLINK-39523 前移到 FLINK-39522 之前，经讨论判定不必要：保持原顺序即可。
- 用户提 fix J 应拆 J.1（OutputWriter 内部）+ J.2（Integration），曾改成 15 步细粒度。
- 最终用户定下 §0 Fix 规范：每个原始 JIRA 最多 1 个 fix，捆绑所有针对该 JIRA 的改动。
  10 步终稿：FLINK-39519 → FLINK-39520 → FLINK-39520 的 fix → FLINK-39521 → FLINK-39521 的 fix →
  FLINK-39522 → FLINK-39522 的 fix → FLINK-39523 → FLINK-39523 的 fix → FLINK-39524。

---

## 7. 总结表

| # | 代码位置 | 当前状态 | 处理 |
|---|---------|---------|------|
| A | `LocalInputChannel.checkpointStarted` 遍历 `toBeConsumedBuffers` | 死代码，错误持久化 FullyFilledBuffer 切片 | 恢复为 emptyList（配合 §4.1） |
| B | `RemoteInputChannel.checkReadability` | 死代码 hack | 删除，调用点回退 |
| C | `RemoteInputChannel` 网络 inflight snapshot | 有效 | 保留 |
| D | `RecoveredInputChannel.onRecoveredStateBuffer` | 死代码包装 | 删除 |
| E | null 守卫 + `instanceof` + 构造器 `!isEmpty()` 守卫丢数据风险 | 架构瑕疵 + 潜在 bug | EMPTY 单例（§2.7） |
| F | `Local.getNextRecoveredBuffer` 优先事件分支 | 有效 | 保留 |
| G | `Local.getBuffersInUseCount` 加 `toBeConsumedBuffers.size()` | 有效（独立于 recovered） | 保留 |
| H | `Local.releaseAllResources` 回收 `toBeConsumedBuffers` | 有效 | 保留 |
| I | Remote 构造器迁移循环 | 已删除 | 确认无残留 |
| J | OutputWriter disk 数据 checkpoint | 未实现 | §4.3 新设计 |
| K | `ChannelStatePersister.startPersisting` 集中化 | 未实现 | §4.1 |
| L | `disk + network inflight` 不共存不变量 | 天然成立 | §2.6：依赖 `UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true` + `getNextBuffer` store 优先 poll；`ChannelStatePersister.startPersisting` 加 `checkState` 防御性兜底 |
