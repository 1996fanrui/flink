# 错误类型 1: Mailbox loop interrupted before recovery was finished

## 根本原因

`java.lang.IllegalStateException: Mailbox loop interrupted before recovery was finished.`

恢复过程中 Mailbox 循环被中断。

## 涉及测试用例

| 测试用例 | 耗时 |
|---------|------|
| `downscale broadcast from 5 to 2, sourceSleepMs = 0` | 2.375s |
| `downscale pipeline from 21 to 20, sourceSleepMs = 0` | 0.341s |

## 完整堆栈

```
Caused by: java.lang.IllegalStateException: Mailbox loop interrupted before recovery was finished.
	at org.apache.flink.util.Preconditions.checkState(Preconditions.java:193)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.restoreInternal(StreamTask.java:838)
	at org.apache.flink.streaming.runtime.tasks.StreamTask.restore(StreamTask.java:786)
	at org.apache.flink.runtime.taskmanager.Task.runWithSystemExitMonitoring(Task.java:987)
	at org.apache.flink.runtime.taskmanager.Task.restoreAndInvoke(Task.java:959)
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:774)
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:579)
	at java.base/java.lang.Thread.run(Thread.java:829)
```

## 关键代码位置

- `StreamTask.restoreInternal` (StreamTask.java:838)
- `StreamTask.restore` (StreamTask.java:786)
