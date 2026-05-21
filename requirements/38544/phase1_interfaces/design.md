# 设计：Phase 1 — 公共接口与 sentinel 类型骨架

> 范围：只引入 [`simplify_approach/overview.md`](../simplify_approach/overview.md) §6 描述的三个跨线程 Java 接口、`RecoveryCheckpointBarrier` sentinel、`DiskSnapshot` 骨架、`ChannelStateWriter.addInputDataFromSpill` 签名；同时把 `RecoveredInputChannel.releaseAllResources()` 提升为 `public`。**不引入任何业务逻辑、不修改任何运行时行为**。Phase 2 与 Phase 3/4 完成后，本 phase 引入的所有骨架方法/字段都将获得真正的调用方与实现；这之间允许存在"只有声明、暂无内部调用方"的中间态，本设计仅承担"通过编译 + 两侧可以并行依赖" 的目标。

## 1. 设计目标

满足 [`user_requirements.md`](./user_requirements.md) 中 REQ-AYII ~ REQ-KX4N。落完本 phase 后：

- Phase 2（InputChannel 侧）可以以"channel `implements RecoverableInputChannel`"的形态展开 spike 与 mock
- Phase 3（spill writer）/ Phase 4（spill reader）可以分别以 `BufferRequester` / `RecoveryCheckpointTrigger` 接口形态展开 spike 与 mock
- 整个 FLINK-38544 主分支可继续编译，无运行时行为变化，所有现有测试不需改动即继续通过

## 2. 文件清单

新增 6 个文件，修改 2 个现有文件。

| 路径 | 操作 | 内容 |
|---|---|---|
| `flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/RecoveryCheckpointTrigger.java` | 新建 | 接口声明 + javadoc |
| `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoverableInputChannel.java` | 新建 | 接口声明 + javadoc |
| `flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/BufferRequester.java` | 新建 | 接口声明 + javadoc |
| `flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/RecoveryCheckpointBarrier.java` | 新建 | sentinel `Buffer` 骨架 + `getCheckpointId()` |
| `flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/DiskSnapshot.java` | 新建 | 类骨架 + 内部 `Chunk` 数据类 |
| `flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateWriter.java` | 修改 | 接口追加 `addInputDataFromSpill`，default no-op |
| `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoveredInputChannel.java` | 修改 | `releaseAllResources()` 由 package-private 改 `public` |
| `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/ChannelStateWriterNoOp.java`（若存在）/ 其他 `ChannelStateWriter` 实现 | 修改 | 实现侧若不继承 default 方法，需要补 no-op override（按现有实现表确定） |

注：`ChannelStateWriter` 的所有现有实现（如 `ChannelStateWriterImpl`、`ChannelStateWriter.NO_OP`、单测 fake 等）必须在本 phase 同步提供 no-op 或最小 override，保证编译通过。具体实现表在开发时通过 `grep -rn "implements ChannelStateWriter\|extends.*ChannelStateWriter" flink-runtime/src` 定位。

## 3. 接口与骨架内容

### 3.1 `RecoveryCheckpointTrigger`

```java
@Internal
public interface RecoveryCheckpointTrigger {
    DiskSnapshot snapshotAndInsertBarriers(long checkpointId);
}
```

javadoc：复用 `simplify_approach/overview.md` §6.1 的语义，至少覆盖：

- 4 步原子行为：取锁 → 拍 `DiskSnapshot.startPos` → 对每个 channel 调用 `onRecoveredStateBuffer(new RecoveryCheckpointBarrier(checkpointId))` → 出锁
- `checkpointId` 由 caller（Phase 5 dispatcher）从触发本 checkpoint 的 `CheckpointBarrier.getId()` 透传
- 调用方锁前置条件（**禁止持有** `SpillFileReader.lock`）
- 返回值用途：作为 Step 3 `addInputDataFromSpill` 的入参

注：simplify_approach `overview.md` §6.1 在文本示例里省略了该参数，但 §6.4 表格中 `RecoveryCheckpointBarrier` 明确"携带 cpId 的 sentinel"——本 phase 落地接口签名时补齐，避免后续 phase 需要再改签名引发"中间态废弃"。

### 3.2 `RecoverableInputChannel`

```java
@Internal
public interface RecoverableInputChannel {
    void onRecoveredStateBuffer(Buffer buffer);
    void finishReadRecoveredState() throws IOException;
}
```

javadoc：复用 §6.2，至少覆盖：

- `onRecoveredStateBuffer`：调用方**必须**持有 `SpillFileReader.lock`；channel 已 release 时静默回收
- `finishReadRecoveredState`：end-of-drain 例外，调用方**无须**持有 `SpillFileReader.lock`；只翻转 `allRecoveredBuffersDelivered` 标志，channel 在标志为 true 且 `recoveredBuffers` 空时完成 `stateConsumedFuture`

### 3.3 `BufferRequester`

```java
@Internal
public interface BufferRequester {
    Buffer requestBufferBlocking(InputChannelInfo channelInfo)
            throws InterruptedException, IOException;
    void releaseExclusiveBuffers() throws IOException;
}
```

javadoc：复用 §6.3，至少覆盖：

- `requestBufferBlocking`：调用方禁止持有 `SpillFileReader.lock`，内部 park 在 `BufferManager.bufferQueue`
- `releaseExclusiveBuffers`：end-of-drain 单线程调用，无锁

### 3.4 `RecoveryCheckpointBarrier`

骨架阶段允许选用以下两种实现形式之一（具体由 Phase 2 / 5 实施时决定，本 phase 锁定接口约束）：

- **方式 A**：实现 `org.apache.flink.runtime.io.network.buffer.Buffer` 接口，必需的方法（`getMemorySegment()` 等）代理到一段空的占位 `MemorySegment` 或抛 `UnsupportedOperationException`，仅在 task 内部短暂存活，不进入真实读写路径
- **方式 B**：继承现有 `Buffer` 实现（如 `NetworkBuffer`），仅扩展 `getCheckpointId()`

Phase 1 默认采用方式 A，落到 `org.apache.flink.runtime.checkpoint.channel` 包内（与 `RecoveryCheckpointTrigger` 同包，方便后续在 lock 内组装）。骨架阶段没有 producer / consumer。

`getCheckpointId()` 直接返回构造时传入的 `cpId`，无并发。

### 3.5 `DiskSnapshot`

```java
public final class DiskSnapshot implements CloseableIterator<DiskSnapshot.Chunk> {
    public static final class Chunk {
        public final InputChannelInfo channelInfo;
        public final byte[] data;
        public final int length;
        Chunk(InputChannelInfo info, byte[] data, int length) { ... }
    }
    public static final class StartPos {        // Phase 4 在 lock 内填实迭代起点；本 phase 仅占位声明
        public final int segmentIndex;
        public final long offset;
        public StartPos(int segmentIndex, long offset) { ... }
    }
    public static DiskSnapshot empty();   // 返回 hasNext()=false / next() 抛 NSEE / close() no-op 的实例，供 Phase 5 feature-off 路径复用
    // 骨架阶段：默认构造也返回 empty 状态
    // 真实迭代逻辑由 Phase 4 引入：根据 startPos 跳过已被 drain 投递的 entry
}
```

类必须 `public final`，构造方法可在骨架阶段保留 package-private（Phase 4 在同包内填充）。`StartPos` 与 `empty()` 都是本 phase 占位声明、Phase 4 填实——避免后续 phase 再追加内部类引发"立即修改" churn。

### 3.6 `ChannelStateWriter.addInputDataFromSpill`

接口追加方法：

```java
void addInputDataFromSpill(long checkpointId,
                           CloseableIterator<DiskSnapshot.Chunk> chunks);
```

骨架阶段在接口处提供 `default` no-op：

```java
default void addInputDataFromSpill(long checkpointId,
                                   CloseableIterator<DiskSnapshot.Chunk> chunks) {
    try { chunks.close(); } catch (Exception ignored) {}
}
```

Phase 5 才会在 `ChannelStateWriterImpl` 中覆盖此方法实现异步 demux 写盘逻辑。其他 mock / fake 实现可继续继承 default。`ChannelStateWriter.NO_OP` 必须显式 override 为真正 no-op（不持有 `chunks` 不会抛错，保持与现有 no-op 风格一致）。

### 3.7 `RecoveredInputChannel.releaseAllResources` 可见性提升

由 `void releaseAllResources()` 改为 `public void releaseAllResources()`。无其他改动。

## 4. 不变式

- 整个 Phase 1 引入的所有新声明都属于"骨架"——除可见性提升外，主分支可观测的运行时行为不变
- 所有现有 caller、实现类、测试代码不需修改即可继续编译并通过
- Phase 2 之前不允许有任何 channel 实现 `RecoverableInputChannel`（避免 Phase 1 + Phase 2 之间产生半实现状态）

## 5. 代码组织

- 三个跨线程接口、`RecoveryCheckpointBarrier`、`DiskSnapshot` 全部位于 `flink-runtime` 模块
- `RecoverableInputChannel` 放在 `partition.consumer` 包（与 `LocalInputChannel` / `RemoteInputChannel` 同包），其他放在 `checkpoint.channel` 包
- 所有新增类型加 `@Internal`
- **提交策略**：本 phase 与其他 4 个 phase 共用同一开发分支，**不发 PR**；完成后作为**单一 commit** 推到分支，禁止 `git commit --amend` / `git rebase -i` 重写历史。完整规则参 [`../simplify_approach/task_breakdown.md`](../simplify_approach/task_breakdown.md) "Commit 策略" 段

## 6. 兼容性

- 接口只新增、不删除，全部加 default no-op 或显式空 override → 现有调用方 / 实现 / 测试零改动
- 可见性提升属于二进制不兼容的"放宽"操作，源码 / 二进制都向前兼容

## 7. 验证策略

仅依赖编译 + 现有 unit / integration test 套不需修改即继续通过；本 phase 不新增测试。具体验收命令见 [`acceptance_test.md`](./acceptance_test.md)。

## 8. 已驳回的替代方案

- **把三个接口合并成一个大接口**：违反 `simplify_approach/overview.md` §6 的关注点分离（trigger / channel / buffer pool 三类语义边界完全不同），且会让 Phase 2 / Phase 3 / Phase 4 失去并行入口
- **`RecoveryCheckpointBarrier` 直接复用 `CheckpointBarrier`**：`CheckpointBarrier` 是 `AbstractEvent`，会被沿正常网络路径序列化并跨 task 传输，本 sentinel 只在单 task 内 task-thread 与 drain 之间短暂使用，不能混淆。必须新建独立类型
- **把 `addInputDataFromSpill` 直接在 Phase 5 时再加到接口上**：会让 Phase 4 的 `SpillFileReader.snapshotAndInsertBarriers` 返回的 `DiskSnapshot` 暂时无下游可调，需要在 Phase 4 中手工 stub 一个第二份接口，违反 simplify approach 的"只一份接口"原则
- **本 phase 顺手实现 `ChannelStateWriterImpl.addInputDataFromSpill` 真正逻辑**：和 Phase 5 的 ref-counter 生命周期、cpId 调度逻辑强耦合，提前实现会形成"立即被改写的中间状态"，违反 CLAUDE.md "禁止引入立即废弃的中间状态" 原则
