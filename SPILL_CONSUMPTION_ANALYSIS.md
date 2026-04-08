# Spill-to-Disk 数据消费方案深入分析

## 核心发现

经过深入代码分析，找到了**最小侵入性的 spill 消费方案**。关键洞察是：
- **RecoveredInputChannel 生命周期长于想象**：不是只在 filtering 阶段，它在 `bufferFilteringCompleteFuture` 完成前一直存活
- **`requestBufferBlocking()` 已有回调机制**：可以触发 disk → buffer 的加载
- **`toInputChannel()` 转换点是关键**：此时 `receivedBuffers` 被迁移到 `LocalInputChannel.toBeConsumedBuffers`

---

## 1. RecoveredInputChannel 的完整生命周期

### 时序关系

```
Recovery Thread (Channel State Unspilling)
    ↓
InputChannelRecoveredStateHandler.recover()
    ├─ for each buffer in checkpoint state:
    │   ├─ if filteringHandler != null:
    │   │   └─ ChannelStateFilteringHandler.filterAndRewrite()
    │   │       └─ channel.requestBufferBlocking()  ← disk spill point
    │   │
    │   └─ channel.onRecoveredStateBuffer(buffer)
    │       └─ receivedBuffers.add(buffer)
    │           └─ if (wasEmpty) notifyChannelNonEmpty()
    │
    └─ channel.finishReadRecoveredState()
        ├─ onRecoveredStateBuffer(EndOfInputChannelStateEvent)
        └─ bufferFilteringCompleteFuture.complete(null)

Task Thread (in parallel, after bufferFilteringCompleteFuture)
    ↓
StreamTask.restoreInternal()
    └─ for each InputGate:
        └─ requestPartitionsTrigger.thenRun(
               () -> inputGate::requestPartitions
           )
            └─ SingleInputGate.requestPartitions()
                └─ convertRecoveredInputChannels()
                    └─ for each RecoveredInputChannel:
                        ├─ RecoveredInputChannel.toInputChannel()
                        │   └─ Extract remainingBuffers from receivedBuffers
                        │       └─ LocalInputChannel.toInputChannelInternal()
                        │           └─ Migrate buffers to toBeConsumedBuffers
                        │
                        ├─ inputChannel.releaseAllResources()
                        └─ Replace in inputChannels map
```

### 关键时间点

| 时间点 | 触发方 | 动作 | 缓冲区状态 |
|--------|--------|------|----------|
| T1 | Recovery Thread | `onRecoveredStateBuffer()` | 缓冲加入 `RecoveredInputChannel.receivedBuffers` |
| T2 | Recovery Thread | `finishReadRecoveredState()` | `bufferFilteringCompleteFuture` 完成 |
| T3 | Task Thread | `requestPartitions()` | `convertRecoveredInputChannels()` 执行 |
| T4 | Task Thread | `getNextBuffer()` | Task 开始消费 |
| T5 | Checkpoint | `checkpointStarted()` | snapshot `LocalInputChannel.toBeConsumedBuffers` |

---

## 2. RecoveredInputChannel 层面的解决方案

### 现状分析

**文件**: `/Users/ruifan/code/github/flink-os-3/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoveredInputChannel.java`

关键方法签名：
```java
// Line 118-137
public final InputChannel toInputChannel() throws IOException {
    Preconditions.checkState(
            bufferFilteringCompleteFuture.isDone(), 
            "buffer filtering is not complete");
    if (!inputGate.isCheckpointingDuringRecoveryEnabled()) {
        Preconditions.checkState(
                stateConsumedFuture.isDone(), 
                "recovered state is not fully consumed");
    }
    
    final ArrayDeque<Buffer> remainingBuffers;
    synchronized (receivedBuffers) {
        remainingBuffers = new ArrayDeque<>(receivedBuffers);
        receivedBuffers.clear();  // ← 关键：缓冲转移点
    }
    
    final InputChannel inputChannel = toInputChannelInternal(remainingBuffers);
    inputChannel.checkpointStopped(lastStoppedCheckpointId);
    return inputChannel;
}

// Line 335-361
public Buffer requestBufferBlocking() throws InterruptedException, IOException {
    if (!exclusiveBuffersAssigned) {
        bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
        exclusiveBuffersAssigned = true;
    }
    if (!inputGate.isCheckpointingDuringRecoveryEnabled()) {
        return bufferManager.requestBufferBlocking();
    }
    
    // ← Heap buffer fallback 在这里
    Buffer buffer = bufferManager.requestBuffer();
    if (buffer != null) {
        return buffer;
    }
    MemorySegment memorySegment = 
        MemorySegmentFactory.allocateUnpooledSegment(MemoryManager.DEFAULT_PAGE_SIZE);
    return new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE);
}

// Line 166-195
public void onRecoveredStateBuffer(Buffer buffer) {
    boolean recycleBuffer = true;
    try {
        final boolean wasEmpty;
        synchronized (receivedBuffers) {
            if (isReleased) {
                wasEmpty = false;
            } else {
                wasEmpty = receivedBuffers.isEmpty();
                receivedBuffers.add(buffer);
                recycleBuffer = false;
            }
        }
        
        if (wasEmpty) {
            notifyChannelNonEmpty();  // ← Task 可以开始消费
        }
    } finally {
        if (recycleBuffer) {
            buffer.recycleBuffer();
        }
    }
}
```

### 侵入性最小的方案：Lazy Disk Buffer Loader

**核心思路**：在 `RecoveredInputChannel` 阶段引入 disk data 的 lazy loading，不修改 `LocalInputChannel` 或 `RemoteInputChannel`。

#### 方案 A：使用 BufferManager 的 BufferListener 机制

**实现点**：在 `RecoveredInputChannel.requestBufferBlocking()` 中添加 listener，当 buffer available 时触发 disk load

```java
// 在 RecoveredInputChannel 中添加

private class DiskBufferLoader {
    private final String[] spillDirectories;
    private final ArrayDeque<File> spilledFiles;  // spilled files on disk
    private boolean hasDiskData = false;
    
    DiskBufferLoader(String[] spillDirectories) {
        this.spillDirectories = spillDirectories;
        this.spilledFiles = new ArrayDeque<>();
    }
    
    void addSpilledFile(File file) {
        spilledFiles.add(file);
        hasDiskData = true;
    }
    
    // 当有 buffer available 时被 BufferManager 回调
    boolean notifyBufferAvailable(Buffer buffer) {
        if (hasDiskData && !spilledFiles.isEmpty()) {
            try {
                File spillFile = spilledFiles.poll();
                Buffer diskBuffer = loadFromDisk(spillFile);
                // 注意：buffer 并不实际消费，只是触发加载
                onRecoveredStateBuffer(diskBuffer);
                return true;
            } catch (IOException e) {
                // handle error
            }
        }
        return false;
    }
    
    private Buffer loadFromDisk(File spillFile) throws IOException {
        // Read from disk and create Buffer
        // 使用 BufferManager.requestBuffer() 获取目标 buffer
        Buffer targetBuffer = bufferManager.requestBuffer();
        if (targetBuffer == null) {
            // Fallback to heap buffer
            MemorySegment segment = 
                MemorySegmentFactory.allocateUnpooledSegment(DEFAULT_PAGE_SIZE);
            targetBuffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        }
        
        // Copy data from disk to buffer
        try (FileInputStream fis = new FileInputStream(spillFile)) {
            fis.getChannel().read(targetBuffer.asByteBuf().nioBuffer());
        }
        
        // Clean up spill file
        spillFile.delete();
        
        return targetBuffer;
    }
}

// 在 RecoveredInputChannel 中：
private final DiskBufferLoader diskBufferLoader = new DiskBufferLoader(...);

public Buffer requestBufferBlocking() throws InterruptedException, IOException {
    if (!exclusiveBuffersAssigned) {
        bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
        exclusiveBuffersAssigned = true;
    }
    
    // 尝试从 disk 加载
    if (diskBufferLoader.hasDiskData && diskBufferLoader.notifyBufferAvailable(null)) {
        return bufferManager.requestBuffer();  // 已被 diskBufferLoader 加入
    }
    
    // 原有逻辑
    if (!inputGate.isCheckpointingDuringRecoveryEnabled()) {
        return bufferManager.requestBufferBlocking();
    }
    
    Buffer buffer = bufferManager.requestBuffer();
    if (buffer != null) {
        return buffer;
    }
    MemorySegment memorySegment = 
        MemorySegmentFactory.allocateUnpooledSegment(MemoryManager.DEFAULT_PAGE_SIZE);
    return new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE);
}
```

**优势**：
1. 完全在 `RecoveredInputChannel` 内部，不修改下游 channel
2. 利用现有的 `requestBufferBlocking()` 调用链
3. Lazy loading：只在需要时才从 disk 读取
4. 自动集成到 checkpoint 机制（buffers 最终进入 `toBeConsumedBuffers`）

#### 方案 B：在 onRecoveredStateBuffer() 前驱动 Disk Load

**实现点**：在 recovery thread 中，当有 spill 数据时，主动触发 load

```java
// 在 InputChannelRecoveredStateHandler 中

private void recoverWithFiltering(
        RecoveredInputChannel channel,
        InputChannelInfo channelInfo,
        int oldSubtaskIndex,
        Buffer retainedBuffer) throws IOException, InterruptedException {
    
    checkState(filteringHandler != null, "filtering handler not set.");
    List<Buffer> filteredBuffers = filteringHandler.filterAndRewrite(
            channelInfo.getGateIdx(),
            oldSubtaskIndex,
            channelInfo.getInputChannelIdx(),
            retainedBuffer,
            channel::requestBufferBlocking);
    
    // 新增：检查是否有 spill 数据
    if (channel instanceof SpillAwareDiskBufferLoader) {
        ((SpillAwareDiskBufferLoader) channel).drainSpilledData();
    }
    
    // 添加过滤后的 buffers
    int i = 0;
    try {
        for (; i < filteredBuffers.size(); i++) {
            channel.onRecoveredStateBuffer(filteredBuffers.get(i));
        }
    } catch (Throwable t) {
        for (int j = i; j < filteredBuffers.size(); j++) {
            filteredBuffers.get(j).recycleBuffer();
        }
        throw t;
    }
}

interface SpillAwareDiskBufferLoader {
    void drainSpilledData() throws IOException, InterruptedException;
}

// RecoveredInputChannel 实现此接口
public class RecoveredInputChannel extends InputChannel 
        implements ChannelStateHolder, SpillAwareDiskBufferLoader {
    
    private final ArrayDeque<File> spilledFiles = new ArrayDeque<>();
    
    @Override
    public void drainSpilledData() throws IOException, InterruptedException {
        while (!spilledFiles.isEmpty()) {
            File spillFile = spilledFiles.poll();
            Buffer diskBuffer = loadFromDisk(spillFile);
            onRecoveredStateBuffer(diskBuffer);
        }
    }
    
    public void registerSpilledFile(File file) {
        spilledFiles.add(file);
    }
    
    private Buffer loadFromDisk(File spillFile) throws IOException {
        // 同方案 A 的逻辑
    }
}
```

**优势**：
1. 更主动的加载策略，不依赖 buffer available 事件
2. 在 filtering 之后立即加载，确保顺序正确
3. 所有 spill 数据都会被消费

---

## 3. SingleInputGate 的 convertRecoveredInputChannels() 时序

**文件**: `/Users/ruifan/code/github/flink-os-3/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/SingleInputGate.java`

```java
// Line 345-357：获取完成 future
@Override
public CompletableFuture<Void> getBufferFilteringCompleteFuture() {
    synchronized (requestLock) {
        List<CompletableFuture<?>> futures = 
            new ArrayList<>(numberOfInputChannels);
        for (InputChannel inputChannel : inputChannels()) {
            if (inputChannel instanceof RecoveredInputChannel) {
                futures.add(
                    ((RecoveredInputChannel) inputChannel)
                        .getBufferFilteringCompleteFuture());
            }
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
}

// Line 398-443：转换阶段
public void convertRecoveredInputChannels() {
    LOG.debug("Converting recovered input channels ({} channels)", 
              getNumberOfInputChannels());
    
    for (Map<InputChannelInfo, InputChannel> inputChannelsForCurrentPartition :
            inputChannels.values()) {
        Set<InputChannelInfo> oldInputChannelInfos = 
            new HashSet<>(inputChannelsForCurrentPartition.keySet());
        
        for (InputChannelInfo inputChannelInfo : oldInputChannelInfos) {
            InputChannel inputChannel = 
                inputChannelsForCurrentPartition.get(inputChannelInfo);
            
            if (!(inputChannel instanceof RecoveredInputChannel)) {
                continue;
            }
            
            try {
                // Phase 1：channel 转换和资源释放
                InputChannel realInputChannel = 
                    ((RecoveredInputChannel) inputChannel).toInputChannel();
                inputChannel.releaseAllResources();
                int buffersInUseCount = realInputChannel.getBuffersInUseCount();
                
                // Phase 2：原子更新数据结构
                synchronized (inputChannelsWithData) {
                    if (inputChannelsWithData.contains(inputChannel)) {
                        inputChannelsWithData.getAndRemove(
                            ch -> ch == inputChannel);
                    }
                    enqueuedInputChannelsWithData.clear(
                        inputChannel.getChannelIndex());
                    
                    inputChannelsForCurrentPartition.remove(inputChannelInfo);
                    inputChannelsForCurrentPartition.put(
                        realInputChannel.getChannelInfo(), 
                        realInputChannel);
                    channels[inputChannel.getChannelIndex()] = 
                        realInputChannel;
                    
                    if (buffersInUseCount > 0) {
                        inputChannelsWithData.add(realInputChannel);
                        enqueuedInputChannelsWithData.set(
                            realInputChannel.getChannelIndex());
                    }
                }
            } catch (Throwable t) {
                inputChannel.setError(t);
                return;
            }
        }
    }
}
```

### 关键洞察

1. **转换时机严格**：只在 `bufferFilteringCompleteFuture` 完成后才调用
   - 确保所有过滤完成
   - 所有 disk spill 数据可以在转换前加载

2. **缓冲区迁移**：通过 `toInputChannelInternal(remainingBuffers)` 传递
   - `RecoveredInputChannel.receivedBuffers` → `LocalInputChannel.toBeConsumedBuffers`
   - 这是唯一的缓冲区转移点

3. **原子性保证**：Phase 2 在 `inputChannelsWithData` 锁下执行
   - 防止 Task 线程在转换中间消费数据

---

## 4. Checkpoint 在 RecoveredInputChannel 阶段的支持

### 现状问题

**文件**: `/Users/ruifan/code/github/flink-os-3/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoveredInputChannel.java:364`

```java
@Override
public void checkpointStarted(CheckpointBarrier barrier) 
        throws CheckpointException {
    throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
}
```

为什么抛异常？
- RecoveredInputChannel 是临时的，不是最终 channel
- 在 recovery 期间就触发 checkpoint 会导致状态不一致
- 转换后的 LocalInputChannel/RemoteInputChannel 才承载最终状态

### 改进方案

**目标**：让 RecoveredInputChannel 在 filtering 完成后支持 checkpoint

```java
// 在 RecoveredInputChannel 中添加

private ChannelStatePersister statePersister;

// 初始化（在 setChannelStateWriter 时）
@Override
public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
    checkState(this.channelStateWriter == null, "Already initialized");
    this.channelStateWriter = checkNotNull(channelStateWriter);
    // 新增：创建 checkpoint 支持
    this.statePersister = new ChannelStatePersister(
        channelStateWriter, 
        getChannelInfo());
}

// 在 checkpoint barrier 到达时
@Override
public void checkpointStarted(CheckpointBarrier barrier) 
        throws CheckpointException {
    // 只有 filtering 完成后才允许 checkpoint
    if (!bufferFilteringCompleteFuture.isDone()) {
        throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
    }
    
    // 获取当前的 received buffers
    List<Buffer> knownBuffers = new ArrayList<>();
    synchronized (receivedBuffers) {
        for (Buffer buffer : receivedBuffers) {
            if (buffer.isBuffer()) {
                knownBuffers.add(buffer.retainBuffer());
            }
        }
    }
    
    // 调用 persister 保存 checkpoint 状态
    statePersister.startPersisting(barrier.getId(), knownBuffers);
}

@Override
public void checkpointStopped(long checkpointId) {
    if (statePersister != null) {
        statePersister.stopPersisting(checkpointId);
    }
    this.lastStoppedCheckpointId = checkpointId;
}
```

### 为什么可行

1. **时序正确**：filtering 完成 → checkpoint 可以开始
2. **状态清晰**：`receivedBuffers` 中的所有 buffers 都已被过滤
3. **无冲突**：RecoveredInputChannel 转换为 LocalInputChannel 时会传递 `lastStoppedCheckpointId`
4. **自动集成**：LocalInputChannel 的现有 checkpoint 逻辑会继承状态

---

## 5. LocalInputChannel 和 RemoteInputChannel 的转换

### LocalInputChannel 的转换

**文件**: `/Users/ruifan/code/github/flink-os-3/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/LocalInputChannel.java:90-142`

```java
public LocalInputChannel(
        SingleInputGate inputGate,
        int channelIndex,
        ResultPartitionID partitionId,
        ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
        ResultPartitionManager partitionManager,
        TaskEventPublisher taskEventPublisher,
        int initialBackoff,
        int maxBackoff,
        Counter numBytesIn,
        Counter numBuffersIn,
        ChannelStateWriter stateWriter,
        ArrayDeque<Buffer> initialRecoveredBuffers) {  // ← 关键参数
    
    // ...
    
    this.channelStatePersister = new ChannelStatePersister(stateWriter, getChannelInfo());
    
    // 迁移恢复的 buffers
    if (!initialRecoveredBuffers.isEmpty()) {
        final int expectedCount = initialRecoveredBuffers.size();
        int seqNum = Integer.MIN_VALUE;
        while (!initialRecoveredBuffers.isEmpty()) {
            Buffer buffer = initialRecoveredBuffers.poll();
            Buffer.DataType nextDataType = 
                initialRecoveredBuffers.isEmpty() 
                    ? Buffer.DataType.NONE
                    : initialRecoveredBuffers.peek().getDataType();
            BufferAndBacklog bufferAndBacklog = 
                new BufferAndBacklog(buffer, 0, nextDataType, seqNum++);
            toBeConsumedBuffers.add(bufferAndBacklog);  // ← 进入消费队列
        }
    }
}
```

### RemoteInputChannel 的转换

**文件**: `/Users/ruifan/code/github/flink-os-3/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RemoteRecoveredInputChannel.java:70-91`

```java
@Override
protected InputChannel toInputChannelInternal(ArrayDeque<Buffer> remainingBuffers)
        throws IOException {
    RemoteInputChannel remoteInputChannel =
            new RemoteInputChannel(
                    inputGate,
                    getChannelIndex(),
                    partitionId,
                    consumedSubpartitionIndexSet,
                    connectionId,
                    connectionManager,
                    initialBackoff,
                    maxBackoff,
                    partitionRequestListenerTimeout,
                    networkBuffersPerChannel,
                    numBytesIn,
                    numBuffersIn,
                    channelStateWriter,
                    remainingBuffers);  // ← 也会接收初始 buffers
    remoteInputChannel.setup();
    return remoteInputChannel;
}
```

---

## 6. BufferManager 的 Buffer Available 回调机制

**文件**: `/Users/ruifan/code/github/flink-os-3/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/BufferManager.java:52-355`

### 核心机制

```java
public class BufferManager implements BufferListener, BufferRecycler {
    
    private final AvailableBufferQueue bufferQueue = new AvailableBufferQueue();
    
    @GuardedBy("bufferQueue")
    private boolean isWaitingForFloatingBuffers;
    
    // Buffer 请求时注册 listener
    private boolean shouldContinueRequest(BufferPool bufferPool) {
        if (bufferPool.addBufferListener(this)) {  // ← 关键：注册 listener
            isWaitingForFloatingBuffers = true;
            numRequiredBuffers = 1;
            return false;
        } else if (bufferPool.isDestroyed()) {
            throw new CancelTaskException("Local buffer pool has already been released.");
        } else {
            return true;
        }
    }
    
    // Buffer available 时的回调
    @Override
    public boolean notifyBufferAvailable(Buffer buffer) {
        if (inputChannel.isReleased()) {
            return false;
        }
        
        int numBuffers = 0;
        boolean isBufferUsed = false;
        try {
            synchronized (bufferQueue) {
                checkState(
                    isWaitingForFloatingBuffers,
                    "This channel should be waiting for floating buffers.");
                isWaitingForFloatingBuffers = false;
                
                if (inputChannel.isReleased() 
                        || bufferQueue.getAvailableBufferSize() 
                           >= numRequiredBuffers) {
                    return false;
                }
                
                bufferQueue.addFloatingBuffer(buffer);  // ← 缓冲加入队列
                isBufferUsed = true;
                numBuffers += 1 + tryRequestBuffers();
                bufferQueue.notifyAll();  // ← 唤醒等待的线程
            }
            
            inputChannel.notifyBufferAvailable(numBuffers);  // ← 通知 channel
        } catch (Throwable t) {
            inputChannel.setError(t);
        }
        
        return isBufferUsed;
    }
}
```

### 如何利用此机制实现 Disk Spill Loading

在 `RecoveredInputChannel` 中：

```java
public class RecoveredInputChannel extends InputChannel 
        implements ChannelStateHolder {
    
    private final DiskSpillManager diskSpillManager;
    
    // 在 requestBufferBlocking() 中注册 disk spill loader
    public Buffer requestBufferBlocking() throws InterruptedException, IOException {
        if (!exclusiveBuffersAssigned) {
            bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
            exclusiveBuffersAssigned = true;
        }
        
        // 尝试从 disk 加载（使用 listener 机制）
        if (diskSpillManager.hasSpilledData()) {
            // 这会触发 BufferManager 的 notifyBufferAvailable 回调
            diskSpillManager.loadNextSpilledBuffer();
            // 加载的 buffer 已通过 onRecoveredStateBuffer() 添加到 receivedBuffers
        }
        
        // 标准流程
        if (!inputGate.isCheckpointingDuringRecoveryEnabled()) {
            return bufferManager.requestBufferBlocking();
        }
        
        Buffer buffer = bufferManager.requestBuffer();
        if (buffer != null) {
            return buffer;
        }
        
        MemorySegment memorySegment = 
            MemorySegmentFactory.allocateUnpooledSegment(
                MemoryManager.DEFAULT_PAGE_SIZE);
        return new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE);
    }
}
```

---

## 7. 完整的侵入性最小方案

### 架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                       Recovery Thread                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  InputChannelRecoveredStateHandler.recover()                     │
│    ├─ ChannelStateFilteringHandler.filterAndRewrite()            │
│    │   └─ channel.requestBufferBlocking()                        │
│    │       └─ [NEW] DiskSpillManager.loadNextSpilledBuffer()    │
│    │           └─ loadFromDisk(spillFile)                        │
│    │               └─ channel.onRecoveredStateBuffer()           │
│    │                   └─ receivedBuffers.add()                  │
│    │                                                             │
│    └─ channel.onRecoveredStateBuffer() [for filtered buffers]   │
│        └─ receivedBuffers.add()                                  │
│                                                                   │
│  channel.finishReadRecoveredState()                              │
│    └─ bufferFilteringCompleteFuture.complete()                   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                    [bufferFilteringCompleteFuture done]
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                       Task Thread                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  StreamTask.restoreInternal()                                    │
│    └─ inputGate.requestPartitions()                              │
│        └─ convertRecoveredInputChannels()                        │
│            └─ RecoveredInputChannel.toInputChannel()             │
│                └─ Extract remainingBuffers                       │
│                └─ LocalInputChannel() + migrate buffers          │
│                    └─ toBeConsumedBuffers.add()                  │
│                        └─ [Including all disk spill data]        │
│                                                                   │
│  getNextBuffer() [Task consumption]                              │
│    └─ LocalInputChannel.getNextBuffer()                          │
│        └─ toBeConsumedBuffers.poll()                             │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
                   [Task processes records]
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Checkpoint Phase                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  checkpointStarted(CheckpointBarrier)                            │
│    └─ LocalInputChannel.checkpointStarted()                      │
│        └─ ChannelStatePersister.startPersisting()                │
│            └─ Snapshot toBeConsumedBuffers (inflight buffers)   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 核心改动点

#### 1. 新增 DiskSpillManager 类

**位置**: `org.apache.flink.runtime.io.network.partition.consumer`

```java
/**
 * Manages disk-spilled data for RecoveredInputChannel during filtering phase.
 * Loads spilled buffers on demand, triggered by requestBufferBlocking() calls.
 */
public class DiskSpillManager {
    
    private final ArrayDeque<File> spilledFiles;
    private final BufferManager bufferManager;
    private final RecoveredInputChannel channel;
    private volatile boolean hasSpilledData;
    
    public DiskSpillManager(
            BufferManager bufferManager, 
            RecoveredInputChannel channel) {
        this.bufferManager = bufferManager;
        this.channel = channel;
        this.spilledFiles = new ArrayDeque<>();
        this.hasSpilledData = false;
    }
    
    /**
     * Register a spilled file for lazy loading.
     */
    public void registerSpilledFile(File file) {
        spilledFiles.add(file);
        hasSpilledData = true;
    }
    
    /**
     * Check if there are spilled buffers waiting to be loaded.
     */
    public boolean hasSpilledData() {
        return hasSpilledData;
    }
    
    /**
     * Load the next spilled buffer, triggered by requestBufferBlocking().
     * Returns true if a buffer was loaded, false if no more spilled data.
     */
    public boolean loadNextSpilledBuffer() throws IOException {
        if (spilledFiles.isEmpty()) {
            hasSpilledData = false;
            return false;
        }
        
        File spillFile = spilledFiles.poll();
        Buffer diskBuffer = loadFromDisk(spillFile);
        if (diskBuffer != null) {
            channel.onRecoveredStateBuffer(diskBuffer);
            return true;
        }
        
        hasSpilledData = spilledFiles.isEmpty() ? false : true;
        return false;
    }
    
    /**
     * Load buffer from disk file.
     * Creates buffer with proper memory management.
     */
    private Buffer loadFromDisk(File spillFile) throws IOException {
        // Request buffer from pool or use heap fallback
        Buffer targetBuffer = bufferManager.requestBuffer();
        if (targetBuffer == null) {
            MemorySegment segment = 
                MemorySegmentFactory.allocateUnpooledSegment(
                    MemoryManager.DEFAULT_PAGE_SIZE);
            targetBuffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        }
        
        // Read from spill file
        try (RandomAccessFile raf = new RandomAccessFile(spillFile, "r")) {
            ByteBuf targetBuf = targetBuffer.asByteBuf();
            int bytesRead = raf.getChannel().read(
                targetBuf.nioBuffer(0, targetBuf.capacity()));
            targetBuf.writerIndex(bytesRead);
        } catch (IOException e) {
            targetBuffer.recycleBuffer();
            throw e;
        } finally {
            spillFile.delete();
        }
        
        return targetBuffer;
    }
}
```

#### 2. 修改 ChannelStateFilteringHandler

**位置**: `org.apache.flink.runtime.checkpoint.channel.ChannelStateFilteringHandler.java`

在 `InputChannelRecoveredStateHandler.recoverWithFiltering()` 中添加 spill 注册逻辑：

```java
// 在 recoverWithFiltering 中
private void recoverWithFiltering(
        RecoveredInputChannel channel,
        InputChannelInfo channelInfo,
        int oldSubtaskIndex,
        Buffer retainedBuffer)
        throws IOException, InterruptedException {
    
    checkState(filteringHandler != null, "filtering handler not set.");
    
    // [NEW] 获取 disk spill 信息
    List<File> spilledFiles = filteringHandler.getSpilledFiles(
        channelInfo.getGateIdx(), oldSubtaskIndex, channelInfo.getInputChannelIdx());
    
    // [NEW] 注册 spilled files 到 channel
    if (channel instanceof DiskSpillAware) {
        for (File spillFile : spilledFiles) {
            ((DiskSpillAware) channel).registerSpilledFile(spillFile);
        }
    }
    
    List<Buffer> filteredBuffers = filteringHandler.filterAndRewrite(
            channelInfo.getGateIdx(),
            oldSubtaskIndex,
            channelInfo.getInputChannelIdx(),
            retainedBuffer,
            channel::requestBufferBlocking);
    
    // 添加过滤后的 buffers
    int i = 0;
    try {
        for (; i < filteredBuffers.size(); i++) {
            channel.onRecoveredStateBuffer(filteredBuffers.get(i));
        }
    } catch (Throwable t) {
        for (int j = i; j < filteredBuffers.size(); j++) {
            filteredBuffers.get(j).recycleBuffer();
        }
        throw t;
    }
}

// [NEW] 接口标记
public interface DiskSpillAware {
    void registerSpilledFile(File file);
}
```

#### 3. 修改 RecoveredInputChannel

**位置**: `org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel.java`

```java
public abstract class RecoveredInputChannel extends InputChannel 
        implements ChannelStateHolder, DiskSpillAware {
    
    // [NEW]
    private final DiskSpillManager diskSpillManager;
    
    // [NEW] 在构造函数中初始化
    RecoveredInputChannel(...) {
        // ...existing init...
        this.diskSpillManager = new DiskSpillManager(bufferManager, this);
    }
    
    // [NEW] 实现 DiskSpillAware
    @Override
    public void registerSpilledFile(File file) {
        diskSpillManager.registerSpilledFile(file);
    }
    
    // [MODIFIED] requestBufferBlocking() 中添加 disk spill 加载
    public Buffer requestBufferBlocking() throws InterruptedException, IOException {
        if (!exclusiveBuffersAssigned) {
            bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
            exclusiveBuffersAssigned = true;
        }
        
        // [NEW] 尝试加载 spilled buffers
        while (diskSpillManager.hasSpilledData()) {
            try {
                if (diskSpillManager.loadNextSpilledBuffer()) {
                    // 已加载到 receivedBuffers，继续检查
                    continue;
                }
            } catch (IOException e) {
                LOG.warn("Failed to load spilled buffer, continuing with normal flow", e);
            }
            break;
        }
        
        if (!inputGate.isCheckpointingDuringRecoveryEnabled()) {
            return bufferManager.requestBufferBlocking();
        }
        
        Buffer buffer = bufferManager.requestBuffer();
        if (buffer != null) {
            return buffer;
        }
        MemorySegment memorySegment =
                MemorySegmentFactory.allocateUnpooledSegment(
                    MemoryManager.DEFAULT_PAGE_SIZE);
        return new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE);
    }
    
    // [NEW] 支持 checkpoint
    @Override
    public void checkpointStarted(CheckpointBarrier barrier) 
            throws CheckpointException {
        // 只有 filtering 完成且没有 spilled 数据时才允许 checkpoint
        if (!bufferFilteringCompleteFuture.isDone()) {
            throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
        }
        
        if (diskSpillManager.hasSpilledData()) {
            // 仍有 spilled 数据未加载，无法 checkpoint
            throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
        }
        
        // 创建 checkpoint persister 并保存状态
        if (channelStateWriter != null) {
            List<Buffer> knownBuffers = new ArrayList<>();
            synchronized (receivedBuffers) {
                for (Buffer buffer : receivedBuffers) {
                    if (buffer.isBuffer()) {
                        knownBuffers.add(buffer.retainBuffer());
                    }
                }
            }
            
            // 使用专用 persister
            ChannelStatePersister persister = new ChannelStatePersister(
                channelStateWriter, getChannelInfo());
            persister.startPersisting(barrier.getId(), knownBuffers);
        }
    }
}
```

---

## 8. 验证清单

### 为何此方案最小化侵入

| 修改点 | 侵入性 | 说明 |
|--------|--------|------|
| 新增 DiskSpillManager | 低 | 独立类，只在 RecoveredInputChannel 中使用 |
| RecoveredInputChannel.registerSpilledFile() | 低 | 新增方法，不改变既有逻辑 |
| RecoveredInputChannel.requestBufferBlocking() | 中 | 添加 spill load 调用，但不改变返回逻辑 |
| RecoveredInputChannel.checkpointStarted() | 中 | 改为有条件支持，而不是直接抛异常 |
| ChannelStateFilteringHandler | 低 | 仅在 recoverWithFiltering 中添加注册逻辑 |
| LocalInputChannel/RemoteInputChannel | 无 | 无修改，完全兼容 |
| BufferManager | 无 | 无修改，利用现有机制 |

### 不修改的原因

1. **LocalInputChannel/RemoteInputChannel**：
   - 已有 `initialRecoveredBuffers` 参数支持恢复 buffers 的初始化
   - Checkpoint 机制已完整（ChannelStatePersister）
   - 无需修改即可处理 disk spill 数据

2. **BufferManager**：
   - 已有 listener 机制，不需要修改
   - DiskSpillManager 通过 `onRecoveredStateBuffer()` 间接利用

3. **SingleInputGate**：
   - 转换逻辑已稳定
   - 只需确保 RecoveredInputChannel 在转换前处理好所有数据

---

## 9. 关键文件路径总结

| 文件 | 功能 | 修改程度 |
|------|------|----------|
| `/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoveredInputChannel.java` | 核心恢复 channel | 中等修改 |
| `/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/DiskSpillManager.java` | [新增] Disk spill 管理 | 新增 |
| `/flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/RecoveredChannelStateHandler.java` | 状态恢复处理 | 低修改 |
| `/flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateFilteringHandler.java` | 过滤处理 | 低修改 |
| `/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/SingleInputGate.java` | 输入门 | 无修改 |
| `/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/LocalInputChannel.java` | 本地 channel | 无修改 |
| `/flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RemoteInputChannel.java` | 远程 channel | 无修改 |

---

## 10. 时间线和完成 Checklist

### 实现步骤

- [ ] **Phase 1**: 创建 DiskSpillManager 类
  - 实现 `registerSpilledFile()` 和 `loadNextSpilledBuffer()`
  - 完成 `loadFromDisk()` 逻辑
  
- [ ] **Phase 2**: 修改 RecoveredInputChannel
  - 添加 DiskSpillManager 字段初始化
  - 实现 DiskSpillAware 接口
  - 修改 `requestBufferBlocking()` 以加载 spilled buffers
  
- [ ] **Phase 3**: 修改 filtering handler
  - 在 `recoverWithFiltering()` 中注册 spilled files
  - 确保调用顺序正确
  
- [ ] **Phase 4**: Checkpoint 支持
  - 修改 `checkpointStarted()` 允许有条件的 checkpoint
  - 添加 ChannelStatePersister 支持
  
- [ ] **Phase 5**: 测试和验证
  - 单元测试：Disk spill loading
  - 集成测试：Recovery + Checkpoint
  - 性能测试：Spill 开销
  
- [ ] **Phase 6**: 文档更新
  - 设计文档
  - 变更日志
  - 配置说明

