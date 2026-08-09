# JIRA 草稿：FilteringHandler#recoverWithFiltering 双重 recycle

## Title

```
Buffer is recycled twice in FilteringHandler#recoverWithFiltering when delivery fails
```

## Description

```
In FilteringHandler#recoverWithFiltering, the catch block recycles buffers starting
at index i:

{code:java}
} catch (Throwable t) {
    for (int j = i; j < filteredBuffers.size(); j++) {
        filteredBuffers.get(j).recycleBuffer();
    }
{code}

But onRecoveredStateBuffer never leaves the buffer un-owned when it throws: it
enqueues the buffer before the only statement that can throw
(notifyChannelNonEmpty()). So the buffer at index i is recycled twice, corrupting
the reference count and the buffer pool. The loop should start at i + 1.

Introduced by FLINK-38930, master only. Reported by [~roman] in
https://github.com/apache/flink/pull/28661.
```

## 备注（不写进 JIRA）

- 修复点：`RecoveredChannelStateHandler.java:331`，`int j = i` → `int j = i + 1`。
- 可选顺手改：`RecoveredInputChannel#onRecoveredStateBuffer` 里的 `traceRecover(...)` 在 `try` 外面，挪进去封死理论泄漏。
- 测试落点：`GateFilterHandlerBufferOwnershipTest` 或 `InputChannelRecoveredStateHandlerTest`。
