# Spill-to-Disk 消费实现指南

## 核心实现代码片段

### 1. DiskSpillManager 完整实现

**文件**: 新增 `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/DiskSpillManager.java`

```java
package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.memory.MemoryManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;

/**
 * Manages disk-spilled buffers for RecoveredInputChannel during filtering phase.
 * 
 * This manager enables lazy loading of disk-spilled data, triggered by the
 * channel's requestBufferBlocking() calls during the recovery-filtering phase.
 * All spilled buffers are integrated into the normal recovery flow through
 * onRecoveredStateBuffer(), ensuring they are properly handled during
 * checkpoint-during-recovery and subsequent consumption.
 */
public class DiskSpillManager {
    private static final Logger LOG = LoggerFactory.getLogger(DiskSpillManager.class);
    
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
     * Registers a spilled file for lazy loading.
     * 
     * @param file The spilled file to be loaded on demand
     */
    public void registerSpilledFile(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        spilledFiles.add(file);
        hasSpilledData = true;
        LOG.debug("Registered spilled file: {}", file.getAbsolutePath());
    }
    
    /**
     * Checks if there are spilled buffers waiting to be loaded.
     * 
     * @return true if there are spilled files, false otherwise
     */
    public boolean hasSpilledData() {
        return hasSpilledData;
    }
    
    /**
     * Loads the next spilled buffer and adds it to the recovery queue.
     * This method is called from requestBufferBlocking() to trigger lazy loading
     * when buffers become available.
     * 
     * @return true if a buffer was successfully loaded, false if no more spilled data
     * @throws IOException if reading from disk fails
     */
    public boolean loadNextSpilledBuffer() throws IOException {
        if (spilledFiles.isEmpty()) {
            hasSpilledData = false;
            return false;
        }
        
        File spillFile = spilledFiles.poll();
        try {
            Buffer diskBuffer = loadFromDisk(spillFile);
            if (diskBuffer != null) {
                // Add loaded buffer to the recovery queue
                channel.onRecoveredStateBuffer(diskBuffer);
                LOG.debug("Successfully loaded spilled buffer: {}", spillFile.getName());
                return true;
            }
        } catch (IOException e) {
            LOG.warn("Failed to load spilled buffer: {}", spillFile.getName(), e);
            // Continue with remaining files
        }
        
        // Update flag based on remaining files
        hasSpilledData = !spilledFiles.isEmpty();
        return false;
    }
    
    /**
     * Loads a buffer from the spilled file on disk.
     * 
     * The method attempts to allocate a buffer from the buffer pool first.
     * If no pool buffer is available, it falls back to an unpooled heap buffer
     * (similar to the heap buffer fallback in RecoveredInputChannel).
     * 
     * @param spillFile The file containing the spilled buffer data
     * @return A buffer containing the loaded data, or null if loading failed
     * @throws IOException if file I/O fails
     */
    private Buffer loadFromDisk(File spillFile) throws IOException {
        // Try to get buffer from pool first
        Buffer targetBuffer = bufferManager.requestBuffer();
        if (targetBuffer == null) {
            // Fallback to unpooled heap buffer (consistent with 
            // RecoveredInputChannel.requestBufferBlocking() behavior)
            MemorySegment segment = 
                MemorySegmentFactory.allocateUnpooledSegment(
                    MemoryManager.DEFAULT_PAGE_SIZE);
            targetBuffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        }
        
        try {
            // Read from spill file
            ByteBuffer nioBuffer = targetBuffer.asByteBuf().nioBuffer();
            try (RandomAccessFile raf = new RandomAccessFile(spillFile, "r");
                 FileChannel channel = raf.getChannel()) {
                
                int bytesRead = 0;
                int totalRead = 0;
                while ((bytesRead = channel.read(nioBuffer)) > 0) {
                    totalRead += bytesRead;
                }
                
                // Update buffer position
                targetBuffer.asByteBuf().writerIndex(totalRead);
                LOG.debug("Loaded {} bytes from spill file: {}", 
                         totalRead, spillFile.getName());
            }
            
            return targetBuffer;
        } catch (IOException e) {
            targetBuffer.recycleBuffer();
            // Remove the spill file after failed read attempt
            try {
                spillFile.delete();
            } catch (Exception deleteEx) {
                LOG.warn("Failed to delete spill file: {}", spillFile.getAbsolutePath(), 
                        deleteEx);
            }
            throw e;
        }
    }
    
    /**
     * Performs cleanup of any remaining spilled files.
     * Called when the channel is released or during error handling.
     */
    public void cleanup() {
        for (File spillFile : spilledFiles) {
            try {
                if (spillFile.exists() && !spillFile.delete()) {
                    LOG.warn("Failed to delete spill file: {}", spillFile.getAbsolutePath());
                }
            } catch (Exception e) {
                LOG.warn("Error deleting spill file: {}", spillFile.getAbsolutePath(), e);
            }
        }
        spilledFiles.clear();
        hasSpilledData = false;
    }
}
```

### 2. RecoveredInputChannel 集成点

**文件**: `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoveredInputChannel.java`

```java
// ============ ADDITION 1: Add interface ============
public abstract class RecoveredInputChannel extends InputChannel 
        implements ChannelStateHolder, DiskSpillAware {

    // ============ ADDITION 2: Add fields ============
    private final DiskSpillManager diskSpillManager;
    private ChannelStatePersister checkpointPersister;

    // ============ MODIFICATION 1: Constructor ============
    RecoveredInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn,
            int networkBuffersPerChannel) {
        // ... existing code ...
        
        bufferManager = new BufferManager(inputGate.getMemorySegmentProvider(), this, 0);
        this.networkBuffersPerChannel = networkBuffersPerChannel;
        
        // ADDITION: Initialize DiskSpillManager
        this.diskSpillManager = new DiskSpillManager(bufferManager, this);
    }

    // ============ ADDITION 3: Implement DiskSpillAware ============
    @Override
    public void registerSpilledFile(File file) {
        diskSpillManager.registerSpilledFile(file);
    }

    // ============ ADDITION 4: Initialize checkpoint persister ============
    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        checkState(this.channelStateWriter == null, "Already initialized");
        this.channelStateWriter = checkNotNull(channelStateWriter);
        
        // ADDITION: Create checkpoint persister
        this.checkpointPersister = new ChannelStatePersister(
            channelStateWriter, 
            getChannelInfo());
    }

    // ============ MODIFICATION 2: requestBufferBlocking() ============
    public Buffer requestBufferBlocking() throws InterruptedException, IOException {
        if (!exclusiveBuffersAssigned) {
            bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
            exclusiveBuffersAssigned = true;
        }
        
        // ADDITION: Try to load spilled buffers before requesting new ones
        // This ensures disk data is gradually integrated into receivedBuffers
        while (diskSpillManager.hasSpilledData()) {
            try {
                if (diskSpillManager.loadNextSpilledBuffer()) {
                    // Successfully loaded one buffer, continue trying
                    // (may load multiple buffers if pool has capacity)
                    continue;
                }
            } catch (IOException e) {
                LOG.warn("Failed to load spilled buffer during recovery, continuing", e);
            }
            break;
        }
        
        if (!inputGate.isCheckpointingDuringRecoveryEnabled()) {
            return bufferManager.requestBufferBlocking();
        }
        
        // ... existing heap buffer fallback code ...
        Buffer buffer = bufferManager.requestBuffer();
        if (buffer != null) {
            return buffer;
        }
        MemorySegment memorySegment =
                MemorySegmentFactory.allocateUnpooledSegment(MemoryManager.DEFAULT_PAGE_SIZE);
        return new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE);
    }

    // ============ MODIFICATION 3: checkpointStarted() ============
    @Override
    public void checkpointStarted(CheckpointBarrier barrier) 
            throws CheckpointException {
        // Allow checkpoint only after:
        // 1. Filtering is complete (bufferFilteringCompleteFuture)
        // 2. All spilled data is drained (diskSpillManager.hasSpilledData == false)
        
        if (!bufferFilteringCompleteFuture.isDone()) {
            throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
        }
        
        if (diskSpillManager.hasSpilledData()) {
            // Still have unloaded spilled data - cannot checkpoint yet
            throw new CheckpointException(
                new CheckpointException.CheckpointFailureReason(
                    "Recovered channel still has unloaded spilled data"));
        }
        
        // Collect current inflight buffers for persistence
        if (checkpointPersister != null) {
            List<Buffer> knownBuffers = new ArrayList<>();
            synchronized (receivedBuffers) {
                for (Buffer buffer : receivedBuffers) {
                    if (buffer.isBuffer()) {
                        knownBuffers.add(buffer.retainBuffer());
                    }
                }
            }
            checkpointPersister.startPersisting(barrier.getId(), knownBuffers);
        }
    }

    // ============ ADDITION 5: checkpointStopped() ============
    @Override
    public void checkpointStopped(long checkpointId) {
        this.lastStoppedCheckpointId = checkpointId;
        
        if (checkpointPersister != null) {
            checkpointPersister.stopPersisting(checkpointId);
        }
    }

    // ============ ADDITION 6: Cleanup ============
    @Override
    void releaseAllResources() throws IOException {
        ArrayDeque<Buffer> releasedBuffers = new ArrayDeque<>();
        boolean shouldRelease = false;
        
        synchronized (receivedBuffers) {
            if (!isReleased) {
                isReleased = true;
                shouldRelease = true;
                releasedBuffers.addAll(receivedBuffers);
                receivedBuffers.clear();
            }
        }
        
        if (shouldRelease) {
            bufferManager.releaseAllBuffers(releasedBuffers);
        }
        
        // ADDITION: Cleanup spilled files
        diskSpillManager.cleanup();
    }
}
```

### 3. DiskSpillAware 接口

**位置**: `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/DiskSpillAware.java`

```java
package org.apache.flink.runtime.io.network.partition.consumer;

import java.io.File;

/**
 * Interface for channels that can register and manage spilled disk files.
 * Used during the recovery-filtering phase to integrate disk-spilled buffers.
 */
public interface DiskSpillAware {
    /**
     * Registers a spilled file for lazy loading.
     * 
     * @param file The spilled file to be loaded during recovery
     */
    void registerSpilledFile(File file);
}
```

### 4. ChannelStateFilteringHandler 修改点

**文件**: `flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/RecoveredChannelStateHandler.java`

在 `InputChannelRecoveredStateHandler.recoverWithFiltering()` 中：

```java
private void recoverWithFiltering(
        RecoveredInputChannel channel,
        InputChannelInfo channelInfo,
        int oldSubtaskIndex,
        Buffer retainedBuffer)
        throws IOException, InterruptedException {
    
    checkState(filteringHandler != null, "filtering handler not set.");
    
    // ADDITION: Get spilled files for this channel segment
    List<File> spilledFiles = filteringHandler.getSpilledFiles(
        channelInfo.getGateIdx(), 
        oldSubtaskIndex, 
        channelInfo.getInputChannelIdx());
    
    // ADDITION: Register spilled files with the channel
    if (channel instanceof DiskSpillAware) {
        for (File spillFile : spilledFiles) {
            ((DiskSpillAware) channel).registerSpilledFile(spillFile);
        }
    }
    
    // EXISTING: Filter and rewrite logic
    List<Buffer> filteredBuffers =
            filteringHandler.filterAndRewrite(
                    channelInfo.getGateIdx(),
                    oldSubtaskIndex,
                    channelInfo.getInputChannelIdx(),
                    retainedBuffer,
                    channel::requestBufferBlocking);  // <- This triggers disk load!
    
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
```

---

## 数据流示意

### Recovery 阶段的数据流

```
Disk Spill Files
        ↓
ChannelStateFilteringHandler.filterAndRewrite()
        ↓
channel.requestBufferBlocking()
        ↓
DiskSpillManager.loadNextSpilledBuffer()
        ↓
loadFromDisk() ← 读取磁盘
        ↓
channel.onRecoveredStateBuffer(diskBuffer)
        ↓
receivedBuffers.add(diskBuffer)
        ↓
[+] Filtered Buffers from same call
        ↓
channel.onRecoveredStateBuffer(filteredBuffer)
        ↓
receivedBuffers.add(filteredBuffer)
```

### 转换到 LocalInputChannel

```
RecoveredInputChannel.receivedBuffers (包含所有 disk + filtered buffers)
        ↓
toInputChannel() [After bufferFilteringCompleteFuture]
        ↓
Extract remainingBuffers from receivedBuffers
        ↓
LocalInputChannel(initialRecoveredBuffers=remainingBuffers)
        ↓
LocalInputChannel.toBeConsumedBuffers (最终消费队列)
```

### Checkpoint 快照

```
checkpointStarted(CheckpointBarrier)
        ↓
[Check: bufferFilteringCompleteFuture.isDone()]
[Check: !diskSpillManager.hasSpilledData()]
        ↓
Collect inflight buffers from LocalInputChannel.toBeConsumedBuffers
        ↓
ChannelStatePersister.startPersisting()
        ↓
Checkpoint State Handle (with buffer offsets)
```

---

## 关键设计决策

### 1. 为什么在 requestBufferBlocking() 中加载

**优势**：
- 自然的触发点：filtering handler 在处理数据时调用此方法
- 资源感知：只在有可用 buffer 时加载
- 顺序保证：spill 数据与 filtered buffers 的顺序正确（都通过 onRecoveredStateBuffer）
- 无额外线程：重用现有的 recovery thread

**劣势**（已缓解）：
- 可能延迟：如果 buffer 不可用则不加载 —— 通过 loop 处理
- IO 阻塞：可能阻塞过滤 —— 这是 recovery 阶段，影响不大

### 2. 为什么支持 Checkpoint on RecoveredInputChannel

**原因**：
- `checkpointingDuringRecoveryEnabled` 时，filtering 完成后立即可以 checkpoint
- RecoveredInputChannel 中的 buffers 就是 "inflight buffers"
- 无需等待转换，直接 snapshot

**条件**：
- `bufferFilteringCompleteFuture.isDone()` ：filtering 已完成
- `!diskSpillManager.hasSpilledData()` ：所有 spill 数据已加载

### 3. 为什么不需要修改 LocalInputChannel

**已支持的机制**：
```java
public LocalInputChannel(..., 
        ArrayDeque<Buffer> initialRecoveredBuffers) {
    // 已有逻辑：迁移恢复的 buffers 到 toBeConsumedBuffers
    if (!initialRecoveredBuffers.isEmpty()) {
        // ... buffers 已添加到 toBeConsumedBuffers ...
    }
    
    // 已有 checkpoint 支持
    this.channelStatePersister = new ChannelStatePersister(
        stateWriter, getChannelInfo());
}
```

所有 disk spill buffers 都已在 RecoveredInputChannel 阶段加入 `receivedBuffers`，
转换时自动迁移到 `toBeConsumedBuffers`。

---

## 测试覆盖点

### Unit Tests

```java
// DiskSpillManager 单元测试
class DiskSpillManagerTest {
    @Test
    public void testLoadFromDisk() throws IOException {
        // 创建临时 spill 文件
        // 调用 loadNextSpilledBuffer()
        // 验证返回的 buffer 包含正确的数据
    }
    
    @Test
    public void testMultipleSpilledFiles() throws IOException {
        // 注册多个 spilled files
        // 逐个加载验证
        // 检查 hasSpilledData() 状态变化
    }
    
    @Test
    public void testCleanup() throws IOException {
        // 注册 spilled files
        // 调用 cleanup()
        // 验证文件被删除
    }
}

// RecoveredInputChannel 集成测试
class RecoveredInputChannelSpillTest {
    @Test
    public void testRequestBufferBlockingWithSpill() 
            throws IOException, InterruptedException {
        // 创建 RecoveredInputChannel 并注册 spilled files
        // 调用 requestBufferBlocking()
        // 验证 receivedBuffers 中包含 disk buffers
    }
    
    @Test
    public void testCheckpointWithSpilledData() throws IOException {
        // 完成 filtering
        // 仍有 spilled data 时尝试 checkpoint
        // 应该抛出 CHECKPOINT_DECLINED_TASK_NOT_READY
    }
}
```

### Integration Tests

```java
class ChannelStateRecoveryWithSpillTest {
    @Test
    public void testRecoveryWithFilteringAndSpill() throws Exception {
        // 设置 filtering 和 spill 环境
        // 执行 recovery
        // 验证最终的 toBeConsumedBuffers 包含所有数据
    }
    
    @Test
    public void testCheckpointDuringRecoveryWithSpill() throws Exception {
        // 启用 checkpoint-during-recovery
        // 执行 recovery + checkpoint
        // 验证 checkpoint state handle 正确
    }
}
```

---

## 配置选项（可选）

如果需要调整 spill 加载行为，可以添加：

```java
// 在 CheckpointingOptions 或新的 SpillOptions 中
public static final ConfigOption<Integer> SPILL_BUFFER_LOAD_BATCH_SIZE =
    ConfigOptions.key("recovery.spill.buffer-load-batch-size")
        .intType()
        .defaultValue(1)
        .withDescription("Number of spilled buffers to load in each requestBufferBlocking() call");

public static final ConfigOption<Integer> SPILL_BUFFER_LOAD_TIMEOUT_MS =
    ConfigOptions.key("recovery.spill.buffer-load-timeout-ms")
        .intType()
        .defaultValue(5000)
        .withDescription("Timeout for loading a spilled buffer from disk");
```

---

## 性能考量

### Memory 使用
- **恢复阶段**：与原来相同（所有 buffers 最终都进入 receivedBuffers）
- **Checkpoint 期间**：无额外 memory（snapshot 包含的是 inflight buffers，而不是 disk 数据）

### Disk I/O
- **总量**：与原来相同（同样的 spill 数据要被读取）
- **时序**：从异步加载改为同步加载（在 recovery thread 中）
  - 优势：无需额外线程
  - 权衡：recovery 阶段 IO 延迟更高
  - 可接受：recovery 是一次性的，启动时间增加是可容忍的

### CPU
- 同步 IO 可能增加 CPU context switch
- 但总体影响不大（IO 会成为 bottleneck）

