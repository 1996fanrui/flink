# Error Analysis: Mailbox loop interrupted before recovery was finished

## 1. 错误描述

```
java.lang.IllegalStateException: Mailbox loop interrupted before recovery was finished.
    at org.apache.flink.util.Preconditions.checkState(Preconditions.java:193)
    at org.apache.flink.streaming.runtime.tasks.StreamTask.restoreInternal(StreamTask.java:838)
```

## 2. 根本原因

**问题根因：检查的是错误的 Future**

在 `StreamTask.restoreStateAndGates()` 中：

```java
// 当前代码（有bug）
return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]))
        .thenRun(mailboxProcessor::suspend);  // 返回的是 thenRun 的结果！
```

在 `StreamTask.restoreInternal()` 中：

```java
CompletableFuture<Void> allGatesRecoveredFuture =
        actionExecutor.call(() -> restoreStateAndGates(initializationMetrics));

mailboxProcessor.runMailboxLoop();

checkState(
        allGatesRecoveredFuture.isDone(),  // 检查的是 thenRun 返回的 future
        "Mailbox loop interrupted before recovery was finished.");
```

**问题分析：**

1. `CompletableFuture.thenRun(action)` 返回一个**新的 Future**，这个新 Future 在 `action` 执行完成后才会完成
2. `suspend()` 在 channel-state-unspilling 线程上执行，发送 poison mail 后立即返回
3. Mailbox 主线程收到 poison mail，设置 `suspended = true`，循环退出
4. 此时 `suspend()` 可能还未返回（跨线程执行），所以 `thenRun` 返回的 future 的 `isDone()` 仍然是 `false`
5. `checkState` 失败，抛出异常

**时序图：**

```
channelIOExecutor 线程               Mailbox 主线程
        |                                   |
  finishReadRecoveredState()                |
        |                                   |
  bufferFilteringCompleteFuture.complete()  |
        |                                   |
  [allOf future 完成]                       |
        |                                   |
  执行 thenRun callback:                    |
    suspend() 开始执行                      |
    sendPoisonMail()                        |
        |  --------------------------->  收到 poison mail
        |                               suspended = true
        |                               退出 runMailboxLoop()
        |                               检查 isDone() → false ← BUG!
    suspend() 返回                          |
  [thenRun future 完成]                     |
        |                               (但已经太晚了)
```

## 3. 修复方案

**修改 `StreamTask.restoreStateAndGates()`**：返回 `allOf` 的结果，而不是 `thenRun` 的结果。

```java
// 修复后的代码
CompletableFuture<Void> allRecoveredFuture =
    CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]));

// thenRun 的副作用仍然需要（触发 suspend）
allRecoveredFuture.thenRun(mailboxProcessor::suspend);

// 返回 allOf 的 future，而不是 thenRun 的 future
return allRecoveredFuture;
```

## 4. 修改文件

- `flink-runtime/src/main/java/org/apache/flink/streaming/runtime/tasks/StreamTask.java`
  - 修改 `restoreStateAndGates()` 方法的返回值

## 5. 验证方法

```bash
./mvnw test -pl flink-tests -Dtest=UnalignedCheckpointRescaleITCase \
    -Dsurefire.failIfNoSpecifiedTests=false
```
