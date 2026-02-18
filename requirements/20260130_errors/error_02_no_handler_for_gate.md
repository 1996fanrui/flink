# 错误类型 2: No handler for gateIndex - 非网络输入不应有恢复的 buffer

## 根本原因

`java.lang.IllegalStateException: No handler for gateIndex 1. This gate is not a network input and should not have recovered buffers.`

在恢复 channel state 时，发现某个 gate 不是网络输入，但却有需要恢复的 buffer，这是不合法的状态。

## 涉及测试用例

| 测试用例 | 耗时 |
|---------|------|
| `downscale union from 2 to 1, sourceSleepMs = 0` | 1.207s |
| `downscale union from 3 to 2, sourceSleepMs = 0` | 1.475s |
| `downscale union from 5 to 3, sourceSleepMs = 5` | 1.909s |

## 完整堆栈

```
Caused by: org.apache.flink.runtime.taskmanager.AsynchronousException: Unable to read channel state
	at org.apache.flink.streaming.runtime.tasks.StreamTask$StreamTaskAsyncExceptionHandler.handleAsyncException(StreamTask.java:1759)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$restoreStateAndGates$6(StreamTask.java:903)
	at org.apache.flink.util.MdcUtils.lambda$wrapRunnable$1(MdcUtils.java:70)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
	at java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.lang.IllegalStateException: No handler for gateIndex 1. This gate is not a network input and should not have recovered buffers.
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateFilteringHandler.filterAndRewrite(ChannelStateFilteringHandler.java:447)
	at org.apache.flink.runtime.checkpoint.channel.InputChannelRecoveredStateHandler.recoverWithFiltering(RecoveredChannelStateHandler.java:190)
	at org.apache.flink.runtime.checkpoint.channel.InputChannelRecoveredStateHandler.recover(RecoveredChannelStateHandler.java:142)
	at org.apache.flink.runtime.checkpoint.channel.InputChannelRecoveredStateHandler.recover(RecoveredChannelStateHandler.java:75)
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateChunkReader.readChunk(SequentialChannelStateReaderImpl.java:237)
	at org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReaderImpl.readSequentially(SequentialChannelStateReaderImpl.java:133)
	at org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReaderImpl.read(SequentialChannelStateReaderImpl.java:119)
	at org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReaderImpl.readInputData(SequentialChannelStateReaderImpl.java:78)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.lambda$restoreStateAndGates$6(StreamTask.java:901)
```

## 关键代码位置

- `ChannelStateFilteringHandler.filterAndRewrite` (ChannelStateFilteringHandler.java:447)
- `InputChannelRecoveredStateHandler.recoverWithFiltering` (RecoveredChannelStateHandler.java:190)
- `InputChannelRecoveredStateHandler.recover` (RecoveredChannelStateHandler.java:142)
- `SequentialChannelStateReaderImpl.readInputData` (SequentialChannelStateReaderImpl.java:78)

## 问题分析

这个错误发生在 union 类型的 rescale 场景。当 rescale 后，某些 gate 从网络输入变成了非网络输入（如 SourceInputWithCheckpoints），但 checkpoint 中仍保存了该 gate 的 buffer 状态，导致恢复时找不到对应的 handler。
