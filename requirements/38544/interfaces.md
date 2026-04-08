# Interfaces — FLINK-38544 OutputWriter & RecoveredBufferStore

## OutputWriter

Decouples filtering from buffer/disk management. filterAndRewrite only calls `write()` — it does not know about buffers, disk, or InputChannel.

```java
public interface OutputWriter extends AutoCloseable {

    /**
     * Write filtered bytes to the target channel.
     *
     * <p>OutputWriter internally handles:
     * - Channel change detection: if channelInfo differs from previous call,
     *   flush current active buffer before writing.
     * - P3 eager drain: before writing, replay disk data to stores while
     *   non-blocking buffer requests succeed.
     * - writeToBackend: fill active buffer (P1), or spill to disk (P2) when
     *   no buffer available. Backend can only downgrade (buffer → file) within
     *   one call, never upgrade.
     *
     * <p>channelInfo is the NEW (post-rescaling) target channel identity.
     *
     * @param data   byte array containing filtered record data (from outputSerializer.getSharedBuffer())
     * @param length number of bytes to write (actual data length, array may be larger)
     * @param channelInfo target channel (post-rescaling identity)
     */
    void write(byte[] data, int length, InputChannelInfo channelInfo)
            throws IOException, InterruptedException;

    /**
     * Flush the active buffer's partial data to the target channel's
     * RecoveredBufferStore.
     *
     * <p>Called before finishReadRecoveredState(). After flush(), no more
     * write() calls are allowed.
     *
     * <p>Why needed: OutputWriter accumulates data in an active buffer. When
     * filtering ends, the buffer may be partially filled. flush() sends this
     * partial buffer to the store so it is available for consumption and
     * checkpoint before channel conversion.
     */
    void flush() throws IOException;

    /**
     * Blocking drain: load all remaining disk data into target stores,
     * then cleanup spill files and mark all stores as complete.
     *
     * <p>Call sequence: flush() → finishReadRecoveredState() → close().
     * close() runs concurrently with Task thread consumption and checkpoint
     * on converted InputChannels.
     *
     * <p>Idempotent: second call is no-op.
     */
    void close() throws IOException, InterruptedException;
}
```

## RecoveredBufferStore

Per-channel buffer store. Holds ready-to-consume buffers. Used by both RecoveredInputChannel (during recovery) and LocalInputChannel/RemoteInputChannel (after conversion). Hides all disk details from InputChannel.

```java
public interface RecoveredBufferStore {

    // ---- Consumption (called by InputChannel on Task thread) ----

    /**
     * Non-blocking take. Returns next ready buffer, or null if no ready buffer
     * available (disk data may still be loading by the drain loop).
     *
     * <p>Called by InputChannel.getNextBuffer(). InputChannel checks this
     * before its own data source (subpartitionView / receivedBuffers).
     */
    @Nullable
    Buffer tryTake();

    /**
     * Peek the data type of the next available buffer without consuming.
     * Returns NONE if empty.
     *
     * <p>Used by InputChannel to construct BufferAndAvailability.nextDataType
     * and to correct nextDataType when priority events are handled.
     */
    Buffer.DataType peekNextDataType();

    // ---- State queries ----

    /**
     * True when no ready buffers AND no pending disk data for this channel.
     *
     * <p>InputChannel uses this to decide whether to fall through to its
     * next data source (subpartitionView for Local, receivedBuffers for Remote).
     *
     * <p>Note: isEmpty() can return true temporarily while drain loop is
     * between loading entries. It does NOT mean all data is consumed — check
     * isComplete() for that.
     */
    boolean isEmpty();

    /**
     * True when all data has been consumed AND no more data will ever be added
     * (OutputWriter has called markComplete() after drain finishes).
     *
     * <p>InputChannel uses this to drop the store reference permanently.
     * Once isComplete() returns true, tryTake() will always return null.
     */
    boolean isComplete();

    /**
     * Number of ready buffers in the queue.
     *
     * <p>Used by InputChannel.getBuffersInUseCount() and
     * unsynchronizedGetNumberOfQueuedBuffers().
     */
    int size();

    // ---- Checkpoint ----

    /**
     * Snapshot all remaining data for checkpoint.
     *
     * <p>Two parts:
     * 1. Ready buffers: retain each buffer in the queue, pass to
     *    ChannelStateWriter.addInputData(CloseableIterator&lt;Buffer&gt;).
     * 2. Disk data: for each pending SpillEntry, open InputStream from
     *    SpillFileReader, pass to ChannelStateWriter.addInputData streaming
     *    overload (InputStream + dataLength). Streams from spill file
     *    directly to checkpoint DataOutputStream, without consuming
     *    Network Buffer Pool or heap buffer.
     *
     * <p>Called by InputChannel.checkpointStarted() on the Task thread.
     * May run concurrently with the drain loop (which also reads the same
     * spill file via an independent SpillFileReader instance).
     *
     * @param writer       checkpoint state writer
     * @param checkpointId current checkpoint ID
     * @param channelInfo  channel identity for the checkpoint entry
     */
    void checkpoint(ChannelStateWriter writer, long checkpointId, InputChannelInfo channelInfo)
            throws IOException;

    // ---- Resource cleanup ----

    /**
     * Release all ready buffers (recycle) and cleanup any resources.
     *
     * <p>Called by InputChannel.releaseAllResources(). After this call,
     * tryTake() returns null and isEmpty() returns true.
     */
    void releaseAll();
}
```

## Internal Methods (not on the interface, used by OutputWriter)

These methods are on the RecoveredBufferStore implementation class, not on the public interface. They are called by OutputWriter (recovery thread), not by InputChannel (task thread).

```java
/**
 * Set the notification callback. Called once during initialization.
 * The callback is invoked when addBuffer() adds a buffer to a previously
 * empty queue, waking up the InputChannel to consume.
 *
 * <p>In practice, this wraps InputChannel.notifyChannelNonEmpty() — the
 * same mechanism Flink uses for upstream data arrival notifications.
 *
 * <p>The callback must be updated on channel conversion: RecoveredInputChannel
 * sets its own callback initially; on conversion to LocalInputChannel/
 * RemoteInputChannel, the new channel replaces the callback.
 *
 * @param callback invoked when a buffer is added to an empty queue
 */
void setNotificationCallback(Runnable callback);

/**
 * Add a ready buffer to the store. Called by OutputWriter when:
 * - P1: a full buffer is flushed from the active buffer
 * - P3/drain: a disk chunk is loaded into a Network Buffer
 * - flush(): the partial active buffer is delivered
 *
 * <p>Thread-safe. If the queue was empty before this add, invokes the
 * notification callback to wake up the InputChannel.
 */
void addBuffer(Buffer buffer);

/**
 * Mark the store as complete. Called by OutputWriter.close() after
 * the drain loop finishes. No more addBuffer() calls after this.
 */
void markComplete();

/**
 * Add a pending SpillEntry belonging to this channel.
 * Called by OutputWriter when spilling data to disk (P2 path).
 * Used by isEmpty() to check pending disk data, and by checkpoint()
 * to read each entry from disk via SpillFileReader.
 *
 * @param entry the spill entry containing file reference, offset, and length
 */
void addPendingSpillEntry(SpillEntry entry);

/**
 * Remove a pending SpillEntry after it has been loaded into a buffer.
 * Called by OutputWriter when a disk entry is replayed
 * (P3 drain or close() drain).
 *
 * @param entry the spill entry to remove
 */
void removePendingSpillEntry(SpillEntry entry);
```
