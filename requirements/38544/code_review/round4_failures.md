# Round 4 — Phase 3 fix 后第一次验证

Log: `log/20260522_174706.log`

## 上一轮（[fix][phase3]）已生效

`grep "Drain: no physical channel" log/20260522_174706.log | wc -l` → **0**。`RecoveredChannelStateHandler.beginChannel(channel.getChannelInfo())` 改动让 spillFile 的 key 与 drain 查找 key 一致，rescale 路径下 InputChannelInfo 不再失配。

但 rescale 类 ITCase 仍然 FAILED，drain 阶段抛出**新的**异常：

```
org.apache.flink.runtime.taskmanager.AsynchronousException: Unable to drain recovered channel state
Caused by: java.nio.file.NoSuchFileException:
  /var/folders/.../flink-channel-spill-XXX/spill-segment-0.bin
    at SpillFile$SpillFileSegment.readBytesAt(SpillFile.java:113)
    at SpillFileReader.drain(SpillFileReader.java:120)
```

drain 知道 entry 在 segment-0 偏移 N 处、长度 L，但 `FileChannel.open(path, READ)` 时文件已经被删了。

## 根因 — filter 写完就把 spillFile 物理删除，drain 还没读

`SpillFile` 是用引用计数管的：

```java
public void acquire() { refCount.incrementAndGet(); }
public void release() throws IOException {
    if (refCount.decrementAndGet() == 0) {
        if (cleanedUp.compareAndSet(false, true)) {
            closed = true;
            deleteAllSegments();
        }
    }
}
```

`SpillFileReader` 构造时 `spillFile.acquire()`，drain `close()` 时 `spillFile.release()` —— drain 这一侧 ref-count 是对的。

问题出在 **filter 侧的 close**：

`SpillFileWriter.close()`（filter 写完后 `RecoveredChannelStateHandler.close()` 调用）：
```java
public void close() throws IOException {
    ...
    try {
        accumulator.close();
    } finally {
        spillFile.close();   // ← 这里是 forced cleanup
    }
}
```

`FilteredBufferWriter.close()`（在 `accumulator.close()` 里）：
```java
public void close() throws IOException {
    ...
    try {
        flush();
    } finally {
        spillFile.close();   // ← 这里又是 forced cleanup
    }
}
```

而 `SpillFile.close()` 跟 `release()` 的行为完全不同——它**绕过 refCount 直接删段**：

```java
public void close() throws IOException {
    if (closed) return;
    closed = true;
    if (cleanedUp.compareAndSet(false, true)) {
        deleteAllSegments();   // 物理删除 spill-segment-*.bin
    }
}
```

`SpillFile.close()` 的注释明确写着这是给 "tests and task-manager shutdown" 用的强制 cleanup，但 production 流程里 filter 写完把它当成正常 close 在用，于是 segment 在 drain 阅读之前就被删掉了。

为什么 round1（RejectedExecutionException）那阵子没暴露这个：drain 当时根本就拒绝提交，drain 没跑就遇不到 segment 不存在的问题。我们这一轮把 drain 真正跑起来后立刻撞上来。

## 修复方案 — `SpillFile` 用 refCount 闭环：writer 也要 acquire/release

最干净的做法：

1. `SpillFile` 构造时把 refCount 初始化为 **1**——代表 "writer 持有一份 grant"。  
2. `SpillFileWriter.close()` 把 `spillFile.close()` 改成 `spillFile.release()`，把 writer 那份 grant 还掉。  
3. `FilteredBufferWriter.close()` **不再**调 `spillFile.close()`——SpillFile 生命周期不归 accumulator 管，accumulator 只负责 flush 残余字节。  
4. `SpillFile.close()` 保留语义不变（forced cleanup，给 task 关闭 / 测试 tearDown 用）。

调用顺序示例：
- 构造 SpillFile：refCount = 1（writer）
- 构造 SpillFileReader → `spillFile.acquire()`：refCount = 2（writer + drain）
- filter 写完，`SpillFileWriter.close()` → `spillFile.release()`：refCount = 1（drain）
- drain 跑完 `SpillFileReader.close()` → `spillFile.release()`：refCount = 0 → deleteAllSegments → 段被删

中间 Step 1（recovery-checkpoint 协议）抢段时会额外 acquire，对应 DiskSnapshot.close 时 release，这一段逻辑不动。

写入 FileChannel 句柄延迟到 drain release 才关，不会影响 drain 读取（`SpillFile.readBytesAt` 每次都 `FileChannel.open(path, READ)` 开独立句柄）。

## 涉及文件

- `SpillFile.java` —— 构造器 `refCount.set(1)`（**Phase 3** 引入的）
- `SpillFileWriter.java` —— `close()` 里 `spillFile.close()` → `spillFile.release()`（**Phase 3** 引入的）
- `FilteredBufferWriter.java` —— `close()` 里去掉 `spillFile.close()`（**Phase 3** 引入的）

全部属于 **Phase 3**，下一个修复 commit 仍然是 `[FLINK-38544][fix][phase3]`。

## 待你确认

1. 根因诊断对吗？（writer close 走的是 forced cleanup，绕过 refCount 直接删段）
2. 改动方向 OK 吗？（SpillFile 构造 refCount=1；SpillFileWriter.close() 走 release；FilteredBufferWriter.close() 不再管 SpillFile 生命周期）
3. 同意后我直接进入 [fix][phase3] commit + verify 循环。
