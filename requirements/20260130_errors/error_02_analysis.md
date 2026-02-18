# Error 02 分析: No handler for gateIndex - Union Downscale 场景下的 FilterContext 配置问题

## 问题描述

在运行 `UnalignedCheckpointRescaleITCase` 测试时，Union 类型的 downscale 场景下出现以下异常：

```
java.lang.IllegalStateException: No handler for gateIndex 1. This gate is not a network input and should not have recovered buffers.
    at org.apache.flink.runtime.checkpoint.channel.ChannelStateFilteringHandler.filterAndRewrite(ChannelStateFilteringHandler.java:447)
    at org.apache.flink.runtime.checkpoint.channel.InputChannelRecoveredStateHandler.recoverWithFiltering(RecoveredChannelStateHandler.java:190)
    at org.apache.flink.runtime.checkpoint.channel.InputChannelRecoveredStateHandler.recover(RecoveredChannelStateHandler.java:142)
    ...
```

涉及的测试用例：
- `downscale union from 2 to 1, sourceSleepMs = 0`
- `downscale union from 3 to 2, sourceSleepMs = 0`
- `downscale union from 5 to 3, sourceSleepMs = 5`

## Root Cause 分析

### 1. Union 场景下的 InputGate 结构

在 Union 场景下，有多个 source 被 union 成一个逻辑输入：

```
Source0 ──┐
Source1 ──┼── UnionInputGate ── FailingSink
Source2 ──┘
```

从物理层面看：
- **`getEnvironment().getAllInputGates()`** 返回所有 IndexedInputGate，每个 upstream operator 有一个 gate
- 假设有 3 个 source 被 union，则有 3 个 IndexedInputGate（gateIndex = 0, 1, 2）

从逻辑层面看：
- 这 3 个 IndexedInputGate 在运行时被组合成一个 **UnionInputGate**
- 在 StreamConfig 中，这被视为**一个输入**（inputIndex = 0）

### 2. RecordFilterContext 的构建问题

在 `StreamTask.createRecordFilterContext()` 中：

```java
// 问题代码位置：StreamTask.java:1963-2001
protected RecordFilterContext createRecordFilterContext() {
    ClassLoader cl = getUserCodeClassLoader();
    StreamConfig.InputConfig[] inputs = configuration.getInputs(cl);
    List<StreamEdge> inEdges = configuration.getInPhysicalEdges(cl);

    // numGates = 物理 IndexedInputGate 的数量（例如 3）
    int numGates = getEnvironment().getAllInputGates().length;
    RecordFilterContext.InputFilterConfig[] inputConfigs =
            new RecordFilterContext.InputFilterConfig[numGates];

    for (int i = 0; i < inputs.length; i++) {
        if (inputs[i] instanceof StreamConfig.NetworkInputConfig) {
            StreamConfig.NetworkInputConfig networkInput =
                    (StreamConfig.NetworkInputConfig) inputs[i];
            // 问题：对于 Union，只有一个 NetworkInputConfig，其 inputGateIndex = 0
            int gateIndex = networkInput.getInputGateIndex();
            // 所以只有 inputConfigs[0] 被设置，inputConfigs[1] 和 inputConfigs[2] 为 null
            inputConfigs[gateIndex] = new RecordFilterContext.InputFilterConfig(...);
        }
    }
    // inputConfigs = [config, null, null]
    return new RecordFilterContext(inputConfigs, ...);
}
```

**问题分析**：

| 概念 | 数量 | 含义 |
|------|------|------|
| `StreamConfig.getInputs()` | 1 | 逻辑输入数量（Union 是一个输入） |
| `getAllInputGates()` | 3 | 物理 IndexedInputGate 数量 |
| `NetworkInputConfig.inputGateIndex` | 0 | 第一个 gate 的索引 |
| `inputConfigs` 数组大小 | 3 | 与物理 gate 数量一致 |
| `inputConfigs` 非空元素 | 1 | 只有 `inputConfigs[0]` 被设置 |

### 3. ChannelStateFilteringHandler 的初始化问题

在 `ChannelStateFilteringHandler.createFromContext()` 中：

```java
// 问题代码位置：ChannelStateFilteringHandler.java:286-376
public static <T> ChannelStateFilteringHandler<T> createFromContext(
        RecordFilterContext filterContext, InputGate[] inputGates) {
    // inputGates.length = 3
    GateFilterHandler<T>[] gateHandlers = new GateFilterHandler<?>[inputGates.length];

    for (int gateIndex = 0; gateIndex < inputGates.length; gateIndex++) {
        RecordFilterContext.InputFilterConfig inputConfig =
                filterContext.getInputConfig(gateIndex);
        if (inputConfig == null) {
            // gateIndex = 1, 2 时，inputConfig 为 null，跳过
            // 导致 gateHandlers[1] 和 gateHandlers[2] 也为 null
            continue;
        }
        // 只有 gateIndex = 0 时创建 handler
        gateHandlers[gateIndex] = new GateFilterHandler<>(...);
    }
    return new ChannelStateFilteringHandler<>(gateHandlers);
}
```

结果：`gateHandlers = [handler, null, null]`

### 4. 恢复时的错误

在恢复 channel state 时，`InputChannelInfo.gateIdx` 来自 checkpoint，指向物理的 IndexedInputGate 索引：

```java
// RecoveredChannelStateHandler.java:190
List<Buffer> filteredBuffers =
        filteringHandler.filterAndRewrite(
                channelInfo.getGateIdx(),  // 可能是 1 或 2
                oldSubtaskIndex,
                channelInfo.getInputChannelIdx(),
                buffer,
                () -> channel.requestBufferBlocking());
```

当 `channelInfo.getGateIdx() = 1` 时：

```java
// ChannelStateFilteringHandler.java:445-451
GateFilterHandler<T> gateHandler = gateHandlers[gateIndex];  // null
if (gateHandler == null) {
    throw new IllegalStateException(
            "No handler for gateIndex " + gateIndex +
            ". This gate is not a network input and should not have recovered buffers.");
}
```

### 5. 为什么只在 Union Downscale 场景出现

- **Union 场景**：多个 source 合并为一个 UnionInputGate，导致逻辑输入数量（1）与物理 gate 数量（3）不一致
- **Downscale 场景**：rescale 时会触发 channel state 的恢复，涉及 filtering 逻辑
- **非 Union 场景**：每个逻辑输入对应一个物理 gate，`inputConfigs` 数组与 `gateHandlers` 一一对应，不会出现 null 的情况

## 修复方向

### 问题的本质

问题的本质是 **逻辑输入数量** 与 **物理 gate 数量** 的不一致：

| 场景 | 逻辑输入数量 | 物理 gate 数量 | NetworkInputConfig 数量 |
|------|--------------|----------------|-------------------------|
| 普通单输入 | 1 | 1 | 1 |
| 双输入（connect） | 2 | 2 | 2 |
| Union（3 个 source） | 1 | 3 | 1（gateIndex=0） |

当前代码假设每个 NetworkInputConfig 对应一个物理 gate，但 Union 打破了这个假设。

### 方案 A：修改 createRecordFilterContext()（推荐）

在 `StreamTask.createRecordFilterContext()` 中，需要为 Union 场景下的所有物理 gate 设置相同的配置。

关键思路：遍历 `inPhysicalEdges` 而不是 `inputs`，因为每个 StreamEdge 对应一个物理 gate。

```java
protected RecordFilterContext createRecordFilterContext() {
    ClassLoader cl = getUserCodeClassLoader();
    StreamConfig.InputConfig[] inputs = configuration.getInputs(cl);
    List<StreamEdge> inEdges = configuration.getInPhysicalEdges(cl);

    int numGates = getEnvironment().getAllInputGates().length;
    RecordFilterContext.InputFilterConfig[] inputConfigs =
            new RecordFilterContext.InputFilterConfig[numGates];

    // 遍历所有 StreamEdge，每个 edge 对应一个物理 gate
    // 使用 gateIndex 计数器，与 StreamingJobGraphGenerator 中的逻辑一致
    int gateIndex = 0;
    Set<Integer> processedInputIndex = new HashSet<>();

    for (StreamEdge inEdge : inEdges) {
        // 计算 inputIndex（与 StreamingJobGraphGenerator 中的逻辑一致）
        int inputIndex = inEdge.getTypeNumber() == 0 ? 0 : inEdge.getTypeNumber() - 1;

        if (inputIndex < inputs.length
                && inputs[inputIndex] instanceof StreamConfig.NetworkInputConfig) {
            StreamConfig.NetworkInputConfig networkInput =
                    (StreamConfig.NetworkInputConfig) inputs[inputIndex];

            // 只有第一次遇到这个 inputIndex 时才增加 gateIndex（非 Union 场景）
            // Union 场景下，相同 inputIndex 会有多个 edge，每个都需要设置 config
            TypeSerializer<?> typeSerializer = networkInput.getTypeSerializer();
            StreamPartitioner<?> partitioner = inEdge.getPartitioner();
            int numberOfChannels = getEnvironment().getTaskInfo().getNumberOfParallelSubtasks();

            inputConfigs[gateIndex++] = new RecordFilterContext.InputFilterConfig(
                    typeSerializer, partitioner, numberOfChannels);
        }
    }

    return new RecordFilterContext(inputConfigs, ...);
}
```

**注意事项**：
1. 遍历顺序必须与 `StreamingJobGraphGenerator.setOperatorConfig()` 中的遍历顺序一致
2. `inEdges` 的顺序决定了 gate 的索引
3. 每个 StreamEdge 对应一个 InputFilterConfig

### 方案 B：在 ChannelStateFilteringHandler 中复用配置

在 `ChannelStateFilteringHandler.createFromContext()` 中，当遇到 null 的 inputConfig 时，尝试复用同一逻辑输入的其他 gate 的配置。

这个方案的问题是：
1. 需要额外的逻辑来确定哪些 gate 属于同一个逻辑输入
2. 不同 gate 可能有不同的 partitioner（虽然 Union 场景下通常相同）

**推荐方案 A**，因为它在问题的根源处修复，使 `RecordFilterContext.inputConfigs` 数组正确映射到所有物理 gate。

### 与 StreamingJobGraphGenerator 的一致性

关键是保持与 `StreamingJobGraphGenerator.setOperatorConfig()` 中 `inputGateCount++` 逻辑的一致性：

```java
// StreamingJobGraphGenerator.java:1189-1230
int inputGateCount = 0;
for (final StreamEdge inEdge : inEdges) {
    ...
    if (chainedSource != null) {
        // chained source - 不增加 inputGateCount
    } else {
        // network input
        if (inputConfigs[inputIndex] == null) {
            // 只有第一次遇到这个 inputIndex 时才创建 NetworkInputConfig
            // 并增加 inputGateCount
            inputConfigs[inputIndex] = new NetworkInputConfig(..., inputGateCount++, ...);
        }
        // Union 场景：后续相同 inputIndex 的 edge 不创建新的 NetworkInputConfig
        // 但物理上仍然有对应的 IndexedInputGate
    }
}
```

问题在于：这段代码创建的 `NetworkInputConfig` 只记录了第一个 gate 的索引，后续 Union 的 gate 没有被记录。

修复方案需要确保 `RecordFilterContext.inputConfigs` 数组能正确覆盖所有物理 gate。

## 涉及的代码文件

1. **`StreamTask.java`** - `createRecordFilterContext()` 方法需要修改
2. **`ChannelStateFilteringHandler.java`** - `createFromContext()` 方法依赖正确的 inputConfigs
3. **`RecoveredChannelStateHandler.java`** - `recoverWithFiltering()` 调用 filterAndRewrite

## 验证方式

运行 Union downscale 测试用例：

```bash
./mvnw test -pl flink-tests \
    -Dtest=UnalignedCheckpointRescaleITCase#shouldRescaleUnalignedCheckpoint \
    -DfailIfNoTests=false \
    -P java11-target -P java11 \
    -Dtest.parameter.topology=union
```

## 与当前分支改动的关系

当前分支 `38544/poc` 引入了 channel state filtering 功能，相关的 commit 包括：

1. `4a7e4ab7d84` - [FLINK-38930][checkpoint] Filtering record before processing without spilling strategy
2. `cb80b1d8adb` - poc for the task4 2. 修复 filter 前 buffer 申请完 network memory 导致deadlock 的问题
3. `ee597868ba0` - 完善 recovered input buffer 中 filtered buffers迁移的功能
4. `f2d06617ff0` - 修复 Local Buffer Pool 没有 snapshot 的问题，以及没有优先处理 high priority event 的问题

**这个 bug 是由新引入的 filtering 功能导致的**：
- `ChannelStateFilteringHandler` 是新引入的类
- `RecordFilterContext` 是新引入的类
- `createRecordFilterContext()` 是新引入的方法

在引入 filtering 功能时，没有考虑到 Union 场景下逻辑输入数量与物理 gate 数量不一致的情况。

## 问题优先级

**高**。这是一个阻塞性问题，导致 Union downscale 场景完全无法工作。

## 相关文档

- requirements/38544/fix-design.md - LocalInputChannel 恢复设计
- requirements/38544/buffer-deadlock-fix-design.md - Buffer 死锁修复设计
- requirements/38544/bug-analysis.md - 下游 Task 卡死问题分析

## 总结

| 项目 | 内容 |
|------|------|
| 问题类型 | Bug - 新功能引入的 regression |
| 影响范围 | Union downscale 场景 |
| 根因 | `createRecordFilterContext()` 没有正确处理 Union 场景 |
| 修复方式 | 修改 `createRecordFilterContext()` 遍历 `inPhysicalEdges` 而非 `inputs` |
| 涉及文件 | `StreamTask.java` |
| 测试验证 | `UnalignedCheckpointRescaleITCase` Union downscale 测试用例 |
