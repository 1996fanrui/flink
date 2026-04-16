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
     * Snapshot ready buffers for checkpoint.
     *
     * <p>Retains each buffer in the queue and passes them to
     * ChannelStateWriter.addInputData(CloseableIterator&lt;Buffer&gt;).
     *
     * <p>Disk data (pending spill entries) is NOT checkpointed here.
     * OutputWriter handles disk data checkpoint separately — it waits
     * for all channels to trigger, then does one sequential pass through
     * the spillEntryQueue (see design.md "Checkpoint 实现").
     *
     * <p>Called by InputChannel.checkpointStarted() on the Task thread.
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
 * Set the notification callback (synchronized).
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
 * <p>synchronized to ensure visibility when addBuffer() reads the callback
 * concurrently with channel conversion updating it.
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
 * Increment the pending spill entry count.
 * Called by OutputWriter when spilling data to disk (P2 path).
 * Used by isEmpty() to determine if disk data exists for this channel.
 *
 * <p>The actual SpillEntry objects are owned by OutputWriter's
 * spillEntryQueue, not by the store. The store only tracks
 * the count for isEmpty() checks.
 */
void incrementPending();

/**
 * Decrement the pending spill entry count.
 * Called by OutputWriter when a disk entry is replayed
 * (P3 drain or close() drain) into a buffer.
 */
void decrementPending();
```
