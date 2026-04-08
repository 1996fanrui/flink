# Spill-to-Disk 消费方案 - 快速参考

## 核心方案：3 个要点

```
┌─────────────────────────────────────────────────────────┐
│ 1. 新增 DiskSpillManager 类                             │
│    - registerSpilledFile(File)                          │
│    - loadNextSpilledBuffer()                            │
│    - loadFromDisk(File)                                 │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 2. RecoveredInputChannel.requestBufferBlocking()        │
│    - 在 buffer 请求时触发 disk load                    │
│    - 加载的 buffer 进入 onRecoveredStateBuffer()        │
│    - 最终汇入 receivedBuffers                           │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│ 3. RecoveredInputChannel.toInputChannel()               │
│    - 转换时提取 receivedBuffers 中的所有数据           │
│    - 迁移到 LocalInputChannel.toBeConsumedBuffers       │
│    - Task 消费 + Checkpoint snapshot                    │
└─────────────────────────────────────────────────────────┘
```

## 修改文件速览

### 新增（1个文件）

**DiskSpillManager.java**
```
File：flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/
Class：public class DiskSpillManager
Lines：~200
Methods：
  - registerSpilledFile(File)
  - hasSpilledData()
  - loadNextSpilledBuffer()
  - loadFromDisk(File)
  - cleanup()
```

### 修改（2个文件）

**RecoveredInputChannel.java**
```
Changes：
  [1] 新增字段：
      - private final DiskSpillManager diskSpillManager
      - private ChannelStatePersister checkpointPersister

  [2] 构造函数：
      - 初始化 diskSpillManager = new DiskSpillManager(...)

  [3] setChannelStateWriter()：
      - 初始化 checkpointPersister = new ChannelStatePersister(...)

  [4] registerSpilledFile(File)：[新增]
      - diskSpillManager.registerSpilledFile(file)

  [5] requestBufferBlocking()：[修改]
      - 添加 while loop 调用 diskSpillManager.loadNextSpilledBuffer()

  [6] checkpointStarted(CheckpointBarrier)：[修改]
      - 改为有条件支持（不是直接抛异常）
      - 检查 bufferFilteringCompleteFuture.isDone()
      - 检查 !diskSpillManager.hasSpilledData()
      - 调用 checkpointPersister.startPersisting()

  [7] checkpointStopped(long)：[修改]
      - 调用 checkpointPersister.stopPersisting()

  [8] releaseAllResources()：[修改]
      - 添加 diskSpillManager.cleanup()

Lines Changed：~80
```

**RecoveredChannelStateHandler.java (内部类)**
```
Changes：
  在 InputChannelRecoveredStateHandler.recoverWithFiltering() 中：
  
  [1] 获取 spilled files：
      List<File> spilledFiles = filteringHandler.getSpilledFiles(...)

  [2] 注册 to channel：
      if (channel instanceof DiskSpillAware) {
          ((DiskSpillAware) channel).registerSpilledFile(file)
      }

Lines Changed：~5
```

### 接口（1个文件）

**DiskSpillAware.java** [新增]
```
File：flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/
Interface：public interface DiskSpillAware
Methods：
  - void registerSpilledFile(File file)
```

### 无修改（7+个文件）

- ✅ LocalInputChannel.java
- ✅ RemoteInputChannel.java  
- ✅ SingleInputGate.java
- ✅ BufferManager.java
- ✅ ChannelStateFilteringHandler.java
- ✅ LocalBufferPool.java
- ✅ StreamTask.java

## 关键方法签名

### DiskSpillManager

```java
public class DiskSpillManager {
    // 注册 spill 文件
    public void registerSpilledFile(File file)
    
    // 检查是否有 spill 数据
    public boolean hasSpilledData()
    
    // 加载下一个 spill 文件
    public boolean loadNextSpilledBuffer() throws IOException
    
    // 清理残余文件
    public void cleanup()
}
```

### RecoveredInputChannel 改动

```java
public abstract class RecoveredInputChannel extends InputChannel 
        implements ChannelStateHolder, DiskSpillAware {
    
    // [新增] 实现 DiskSpillAware
    @Override
    public void registerSpilledFile(File file)
    
    // [修改] 在 requestBufferBlocking() 中添加
    while (diskSpillManager.hasSpilledData()) {
        if (diskSpillManager.loadNextSpilledBuffer()) {
            continue;
        }
        break;
    }
    
    // [修改] checkpointStarted() 改为
    if (!bufferFilteringCompleteFuture.isDone()) {
        throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
    }
    if (diskSpillManager.hasSpilledData()) {
        throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
    }
    checkpointPersister.startPersisting(barrier.getId(), knownBuffers);
}
```

## 数据流简明版

```
Recovery Thread:
  filterAndRewrite() 
    → requestBufferBlocking()
      → [NEW] loadNextSpilledBuffer()
        → loadFromDisk(spillFile)
          → onRecoveredStateBuffer(diskBuffer)
            → receivedBuffers.add(diskBuffer)

Task Thread:
  requestPartitions()
    → convertRecoveredInputChannels()
      → toInputChannel()
        → Extract remainingBuffers  ← 包含所有 disk buffers
          → LocalInputChannel(remainingBuffers)
            → toBeConsumedBuffers.add()  ← Task 消费

Checkpoint Thread:
  checkpointStarted()
    → LocalInputChannel.checkpointStarted()
      → Snapshot toBeConsumedBuffers  ← Disk buffers 被 snapshot
```

## 关键条件和约束

### RecoveredInputChannel.checkpointStarted() 允许的条件

```java
condition 1: bufferFilteringCompleteFuture.isDone()
  ↓
  所有过滤已完成

AND

condition 2: !diskSpillManager.hasSpilledData()
  ↓
  所有 spill buffers 已加载到 receivedBuffers

THEN: 可以安全 snapshot inflight buffers
```

### 时序关键点

| 时间点 | 事件 | 状态 |
|--------|------|------|
| T1 | onRecoveredStateBuffer() | buffers 加入 receivedBuffers |
| T2 | finishReadRecoveredState() | bufferFilteringCompleteFuture.complete() |
| T3 | requestPartitions() | convertRecoveredInputChannels() 开始 |
| T4 | toInputChannel() | receivedBuffers 迁移到 toBeConsumedBuffers |
| T5 | getNextBuffer() | Task 开始消费 |
| T6 | checkpointStarted() | Snapshot toBeConsumedBuffers |

**checkpoint-during-recovery 时**：T2 之后 T3 之前可以 checkpoint

## 测试清单

### Unit Tests
- [ ] DiskSpillManager.loadFromDisk() 
- [ ] DiskSpillManager.hasSpilledData()
- [ ] DiskSpillManager.cleanup()
- [ ] RecoveredInputChannel + spill integration

### Integration Tests
- [ ] Recovery with filtering and spill
- [ ] Checkpoint-during-recovery with spill
- [ ] Multiple spilled files ordering
- [ ] Disk IO error handling

### Coverage
- [ ] Normal path: spill → load → consume → checkpoint
- [ ] Error path: IO failure → cleanup
- [ ] Edge case: No spill (backward compat)

## Performance Checklist

- [ ] Memory overhead: 0（同样进入 receivedBuffers）
- [ ] Disk IO: 0 额外（必须读取的 spill data）
- [ ] CPU: 低（同步 IO，但总量不变）
- [ ] Recovery time: +spill_size/disk_bandwidth

## 配置检查

如需调整，可选配置：

```properties
# 未来优化选项（基础版本无需）
recovery.spill.buffer-load-batch-size=1
recovery.spill.buffer-load-timeout-ms=5000
```

## 常见问题速答

| Q | A |
|---|---|
| 为什么修改 checkpointStarted()？ | 因为 filtering 完成后数据已就绪，可以 snapshot |
| spill buffers 被消费吗？ | 是，通过 onRecoveredStateBuffer() → receivedBuffers → toBeConsumedBuffers |
| spill buffers 被 snapshot 吗？ | 是，在 LocalInputChannel.checkpointStarted() 时 |
| 需要修改 LocalInputChannel 吗？ | 不，已有 initialRecoveredBuffers 参数支持 |
| 向后兼容吗？ | 100% 兼容，无现有行为改变 |
| 支持哪些版本？ | Flink 1.18+（有 checkpoint-during-recovery 特性） |

## 集成步骤（顺序）

1. **创建 DiskSpillManager.java**
   - 完整复制实现代码
   - 导入必要的包

2. **创建 DiskSpillAware.java**
   - 简单接口定义

3. **修改 RecoveredInputChannel.java**
   - 添加字段（diskSpillManager, checkpointPersister）
   - 修改构造函数初始化
   - 修改 requestBufferBlocking()
   - 修改 checkpointStarted()
   - 修改 releaseAllResources()
   - 实现 registerSpilledFile()

4. **修改 RecoveredChannelStateHandler.java**
   - 在 recoverWithFiltering() 添加 spilled files 注册

5. **编译和测试**
   - 单元测试
   - 集成测试
   - 性能测试

## 文件参考

| 文件 | 用途 |
|------|------|
| SPILL_CONSUMPTION_ANALYSIS.md | 深度技术分析 |
| SPILL_IMPLEMENTATION_GUIDE.md | 完整代码实现 |
| SPILL_SOLUTION_SUMMARY.md | 最终总结和决策支持 |
| 本文 | 快速参考 |

## 概念映射

| 概念 | 对应文件/类 | 关键方法 |
|------|-----------|----------|
| Spill 管理 | DiskSpillManager | registerSpilledFile(), loadNextSpilledBuffer() |
| Recovery 临时容器 | RecoveredInputChannel | onRecoveredStateBuffer(), toInputChannel() |
| 最终消费队列 | LocalInputChannel | toBeConsumedBuffers |
| Checkpoint | LocalInputChannel | checkpointStarted(), ChannelStatePersister |
| 触发点 | requestBufferBlocking() | Automatic disk load |

---

**总结**：3 个新增/修改点，4 个新增类/接口，完整的技术方案，最小侵入性。

