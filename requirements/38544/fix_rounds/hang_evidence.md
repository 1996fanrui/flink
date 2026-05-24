# Hang Evidence Log — fact-only

> 本文档**只描述观测到的事实**，不包含任何关于根因或修复方向的推测。
> 所有结论必须能从 heap dump / 线程栈 / 临时 log / 代码引用直接验证。

## 1. 复现环境

- 分支：`38544-spilling-v2/20260522-02-poc-address-comments`
- 当前 HEAD：`b2ae5a114c4 [FLINK-38544][docs] JVM hang diagnosis playbook`
- working tree 未提交改动（截至取样时）：
  - `M flink-runtime/.../LocalInputChannel.java`
  - `M flink-runtime/.../RemoteInputChannel.java`
  - `A flink-runtime/.../RecoveredBufferQueue.java`
  - `M rui_tools/loop.sh`
  - `A requirements/38544/recovered_buffer_queue_refactor.md`
  - `A requirements/38544/peek_next_data_type_comparison.md`
- 命令：`bash rui_tools/loop.sh`，内部调 `mvnw test -Dtest=org.apache.flink.test.checkpointing.UnalignedCheckpointITCase,...UnalignedCheckpointRescaleITCase,...UnalignedCheckpointRescaleWithMixedExchangesITCase`
- 复现率：本次会话所有 fresh-build 后的运行（共 3 次）均在同一位置卡死

## 2. 线程栈事实（jstack，连续两份间隔 5 分钟一致）

- 3 个 `Source: source -> long-to-string-map -> failing-map (X/3)#0` 线程：
  ```
  TaskMailboxImpl.take:161
  MailboxProcessor.processMailsWhenDefaultActionUnavailable:380
  MailboxProcessor.processMail:369
  MailboxProcessor.runMailboxLoop:229
  StreamTask.runMailboxLoop:1209
  StreamTask.invoke:1146
  ```
- 3 个 `Sink: sink (X/3)#0` 线程：与 Source 完全相同的 stack frame
- 3 个 `AsyncOperations-thread-1` 线程：
  ```
  CompletableFuture.get:2004
  OperatorSnapshotFinalizer.create:66      <- snapshotFutures.getInputChannelStateFuture().get()
  AsyncCheckpointRunnable.finalizeNonFinishedSnapshots:192
  AsyncCheckpointRunnable.run:124
  ```
- 3 个 `Channel state writer` 线程：`waitAndTakeUnsafe:210`，队列空

## 3. 测试日志事实

- JVM 内部时间停在约 `2025ms` 处不再增长（多次复现一致）
- log 内最后一类有意义的事件：
  ```
  Triggering checkpoint 1
  SourceCoordinator-Source: source snapshotState
  Source operator Snapshotted [LongSplit{...}]
  KryoSerializer scala extensions are not available.
  ```
- 之后所有 RecoveredBufferQueue `[HANG-DIAG] finish` 事件，无任何 `offer` 事件

## 4. Heap dump 事实（MAT OQL 直接读取，路径 `/tmp/agent-tmp/heap/postfix_205729.hprof`）

### 4.1 SingleInputGate（3 个，对应 3 个 sink subtask）

| 字段 | 值（3 个 gate 一致） |
|---|---|
| `gateIndex` | 0 |
| `numberOfInputChannels` | 3 |
| `requestedPartitionsFlag` | true |
| `hasReceivedAllEndOfPartitionEvents` | false |
| `enqueuedInputChannelsWithData.words[0]` | 0x0 |
| `inputChannelsWithData` size | 0 |
| `availabilityHelper.availableFuture.result` | null（incomplete） |
| `closeFuture.result` | null（incomplete） |
| `channelsWithEndOfPartitionEvents.words[0]` | 0x0 |
| `channelsWithEndOfUserRecords.words[0]` | 0x0 |
| `checkpointingDuringRecoveryEnabled` | true |
| `channels` 数组类型组成 | 1 个 LocalInputChannel + 2 个 RemoteInputChannel |

### 4.2 LocalInputChannel（3 个，每个 sink subtask 一个）

| 字段 | 值（3 个 channel 一致） |
|---|---|
| `recoveredQueue.allDelivered` | true |
| `recoveredQueue.sequenceNumber` | -2147483648 (Integer.MIN_VALUE) |
| `recoveredQueue.buffers` ArrayDeque size | 0 |
| `hasPendingPriorityEvent` | false |
| `isReleased` | false |
| `subpartitionView` | 非 null，与对应 `PipelinedSubpartition.readView` 双向匹配 |

### 4.3 RemoteInputChannel（6 个，每个 sink subtask 两个）

| 字段 | 值（6 个 channel 一致） |
|---|---|
| `recoveredQueue.allDelivered` | true |
| `recoveredQueue.sequenceNumber` | -2147483648 |
| `recoveredQueue.buffers` ArrayDeque size | 0 |
| `receivedBuffers` PrioritizedDeque size | 0 |
| `receivedBuffers.numPriorityElements` | 0 |
| `subpartitionView` | N/A（Remote 无此字段） |

### 4.4 与 stuck LocalInputChannel 匹配的上游 PipelinedSubpartition（3 个，由 `channel.subpartitionView.parent` 直接定位）

| 字段 | 值 |
|---|---|
| `readView` | 非 null，与下游 `LocalInputChannel.subpartitionView` 双向匹配 |
| `isFinished` | false |
| `isBlocked` | false |
| `flushRequested` | true |
| `buffers.numberOfElements` (ArrayDeque size) | 14 |
| `buffers.numPriorityElements` | 0 |
| `totalNumberOfBuffers` | 14 |
| `buffersInBacklog` | 12 |

### 4.5 其他 PipelinedSubpartition（6 个，与本任务的 stuck LocalInputChannel 不匹配）

| 字段 | 值（6 个一致） |
|---|---|
| `readView` | 非 null |
| `isFinished` | false |
| `isBlocked` | true |
| `flushRequested` | true |
| `buffers.numberOfElements` | 2 |
| `totalNumberOfBuffers` | 2 |
| `buffersInBacklog` | 0 |

### 4.6 NetworkBuffer dataType 分布（全 heap 共 23 个实例）

| dataType | 数量 |
|---|---|
| `DATA_BUFFER` | 21 |
| `ALIGNED_CHECKPOINT_BARRIER` | 2 |
| 其他类型 | 0 |

## 5. 临时诊断 log 事实（`RecoveredBufferQueue.offer/finish` 入口加 `LOG.info`，仅本次重现）

在 9 个 input channel（3 sink subtask × 3 channel）上观察到：

| 事件类型 | 出现次数 |
|---|---|
| `offer` 调用 | **0**（log 文件全文搜索 `HANG-DIAG] offer` 零结果） |
| `finish` 调用 | 9（每个 channel 1 次，`totalFinished=1, totalOffered=0, bufferQueueSize=0`） |

样例 log 行：
```
2025 RecoveredBufferQueue [Sink: sink (3/3)#0] INFO  [HANG-DIAG] finish ch=InputChannelInfo{gateIdx=0, inputChannelIdx=0} totalFinished=1 totalOffered=0 bufferQueueSize=0
```

## 6. 当前分支的相关代码事实（vs master 的差异，仅列代码事实）

### 6.1 `RecoveredBufferQueue` 新增

- 当前分支新增 `flink-runtime/.../RecoveredBufferQueue.java`：把原本散落在 `LocalInputChannel` / `RecoveredInputChannel` 的三个字段集中到一个组件
  - `Deque<Buffer> buffers`
  - `boolean allDelivered`
  - `int sequenceNumber`
- master **没有这个类**

### 6.2 `LocalInputChannel` 字段差异

| 字段 | master | 当前分支 |
|---|---|---|
| `recoveredQueue: RecoveredBufferQueue` | 不存在 | 新增 |
| `toBeConsumedBuffers: Deque<BufferAndBacklog>` | 存在（构造器同步装入 `initialRecoveredBuffers`） | 存在 |
| `allRecoveredBuffersDelivered: boolean` | 不存在 | 不存在（搬进 `recoveredQueue`） |
| `recoverySequenceNumber: int` | 不存在 | 不存在（搬进 `recoveredQueue`） |

### 6.3 `LocalInputChannel.getNextBuffer` 主干逻辑差异

- master（`getNextBuffer`）：
  ```
  if (!toBeConsumedBuffers.isEmpty()) return getNextRecoveredBuffer();
  // 否则 fall-through 到 subpartitionView.getNextBuffer()
  ```
- 当前分支（`getNextBuffer`）：
  ```
  boolean inRecovery = recoveredQueue.isInRecovery();  // = !allDelivered || !buffers.isEmpty()
  if (inRecovery) {
      if (hasPendingPriorityEvent) return pullPriorityFromSubpartitionView();
      if (recoveredQueue.isEmpty()) return Optional.empty();
      poll → wrapRecoveredBufferAsAvailability()
  }
  // 否则 fall-through 到 subpartitionView.getNextBuffer()
  ```

### 6.4 `finishRecoveredBufferDelivery()` 行为差异

- master：`LocalInputChannel` 无此方法（master 没有「drain 异步声明结束」这个语义）
- 当前分支：
  ```java
  public void finishRecoveredBufferDelivery() {
      synchronized (recoveredQueue) { recoveredQueue.finish(); }
  }
  ```
  即只翻 `allDelivered` 标志位，**不调用任何 notify**。

### 6.5 构造与 buffer 装入差异

- master：`LocalInputChannel` 构造器入参 `initialRecoveredBuffers: Deque<Buffer>`，构造时同步 `for(buf : initialRecoveredBuffers) toBeConsumedBuffers.add(...)` 装入
- 当前分支：构造器不入 `initialRecoveredBuffers`；recovery buffers 由外部通过 `onRecoveredStateBuffer(buf)` 异步 push（内部走 `recoveredQueue.offer(buf)`）

## 7. 第二次诊断 log 事实（加在 `RecoveredInputChannel` 的 `finishReadRecoveredState` 与 `toInputChannel` 入口）

```
RecoveredInputChannel.finishReadRecoveredState ch=... receivedBuffers.sizeBefore=0   (9 次, 全部 0)
RecoveredInputChannel.toInputChannel          ch=... remainingBuffers.size=0          (9 次, 全部 0)
RecoveredBufferQueue.offer                                                            (0 次, log 全文无此事件)
RecoveredBufferQueue.finish                  ch=... totalOffered=0 bufferQueueSize=0  (9 次, 全部 0)
```

事件顺序观测：
1. log 中 `finishReadRecoveredState` 事件先出现（@ ~2153ms 内部时间）
2. log 中 `toInputChannel` 事件后出现（@ ~2156-2158ms）
3. 这之间没有任何 `offer` 事件
4. 9 个 channel 全部 `finish` 时 `totalOffered=0`

代码事实补充：
- `RecoveredInputChannel.finishReadRecoveredState`（`RecoveredInputChannel.java:208-227`）调用 `onRecoveredStateBuffer(EndOfInputChannelStateEvent)` 把 sentinel push 进 `receivedBuffers`
- `RecoveredInputChannel.toInputChannel`（`RecoveredInputChannel.java:113-149`）的 `remainingBuffers = new ArrayDeque<>(receivedBuffers); receivedBuffers.clear()` 在第 124-127 行
- 上述两步之间，**`receivedBuffers` 被清空了**（remainingBuffers.size=0 直接证据）

## 8. master 对应路径的代码事实

- `master/.../LocalInputChannel.java:90-141`：构造器入参 `ArrayDeque<Buffer> initialRecoveredBuffers`，构造时同步把每个 buffer wrap 成 `BufferAndBacklog` 装入 `toBeConsumedBuffers`
- `master/.../LocalRecoveredInputChannel.java:67-81`：`toInputChannelInternal(ArrayDeque<Buffer> remainingBuffers)` 把 `remainingBuffers` 作为构造参数传入
- `master/.../RecoveredInputChannel.java:118-137`：`toInputChannel()` 的 remainingBuffers 提取与当前分支一致
- 当前分支的差异：
  - `current/.../LocalInputChannel.java:147-159`：通过 `onRecoveredStateBuffer(buf)` → `recoveredQueue.offer(buf)` 异步装入
  - `current/.../LocalRecoveredInputChannel.java:67-77`：`toInputChannelInternal()` **无 remainingBuffers 参数**
  - `current/.../RecoveredInputChannel.java:135-147`：构造完物理 channel 后用 `for (Buffer buf : remainingBuffers) rec.onRecoveredStateBuffer(buf)` 推 buffer

## 9. 待回答的开放问题（不在本文档范围、仅列出待查项）

- 在 master 路径下相同场景（sink fresh-start，filter push EndOfInputChannelStateEvent 后），是否也存在「toInputChannel 看到的 remainingBuffers 为空」的现象？如果有，master 如何不卡死？如果无，master 是哪段代码保证了 receivedBuffers 不被在两步之间清空？
- 当前分支这条 fresh-start 路径里，是哪段代码在 `finishReadRecoveredState` push 之后、`toInputChannel` 抓取之前消费走了 EndOfInputChannelStateEvent？
- 当前分支 `requestSubpartitions` 中的 `notifyDataAvailable(view)` 是否被调用过？被调用时 gate `inputChannelsWithData` 与 `enqueuedInputChannelsWithData` 的具体变化？
