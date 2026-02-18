# Bug 分析：下游 Task 卡死 - toBeConsumedBuffers 消费后丢失上游通知

## 问题现象

- 下游 Task (Sink) 空闲，无法消费数据
- 上游 Task (failing-map) 有数据但无法发送
- Job 卡死

## 代码结构

```
上游 Task
└── PipelinedSubpartition
    └── buffers: 11 个待发送的 buffer
    └── readView: 指向下游的连接（不为 null）

下游 Task
└── SingleInputGate
    └── inputChannelsWithData: 空（没有 channel 可消费）
    └── channels[6]: LocalInputChannel
        └── toBeConsumedBuffers: 空（已被消费完）
        └── subpartitionView: 指向上游（不为 null）
```

## 数据流路径

```
RecoveredInputChannel（恢复阶段）
    │
    │ 转换时迁移 buffer
    ▼
LocalInputChannel.toBeConsumedBuffers（本地缓存）
    │
    │ 优先消费
    ▼
subpartitionView（上游连接）
    │
    │ 本地消费完后再消费
    ▼
上游 PipelinedSubpartition.buffers
```

## 正常流程

1. `requestSubpartitions()` 建立与上游的连接
2. `notifyDataAvailable()` 将 channel 加入 `inputChannelsWithData` 队列
3. Task 从队列取出 channel，调用 `getNextBuffer()` 获取数据
4. 如果 `moreAvailable() = true`，channel 重新入队
5. 如果 `moreAvailable() = false`，channel 不入队，等待上游新数据时通知

## Bug 发生流程

```
1. requestSubpartitions() 成功
   └── channel 入队到 inputChannelsWithData ✓

2. Task poll channel
   └── getNextBuffer() 优先消费 toBeConsumedBuffers

3. 消费 toBeConsumedBuffers 最后一个 buffer
   └── 返回 nextDataType = NONE（构造时预设的值）
   └── moreAvailable() = false
   └── ⚠️ channel 不重新入队

4. 结果：
   - inputChannelsWithData 变空
   - 上游 subpartitionView 有 11 个 buffer 等待消费
   - 但 channel 不在队列里，Task 无法获取
   - 💀 卡死
```

## Root Cause

`LocalInputChannel` 构造时迁移 `toBeConsumedBuffers`，最后一个 buffer 的 `nextDataType` 被预设为 `NONE`：

```java
// 构造时：toBeConsumedBuffers 后面没有更多 recovered buffer
nextDataType = initialRecoveredBuffers.isEmpty()
    ? Buffer.DataType.NONE  // ← 问题：没有考虑 subpartitionView 可能有数据
    : initialRecoveredBuffers.peek().getDataType();
```

当 `toBeConsumedBuffers` 消费完后：
- `nextDataType = NONE` → `moreAvailable() = false`
- Channel 不重新入队
- **但此时 `subpartitionView` 有数据，却无法被消费！**

## 修复方向

在 `getNextBuffer()` 返回 `toBeConsumedBuffers` 最后一个 buffer 时，需要动态检查 `subpartitionView` 是否有数据，而不是使用预设的 `nextDataType = NONE`。
