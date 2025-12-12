# The existing logic

When task encounters any exception, `postFailureCleanUpRegistry.close();` do some cleanups.

https://github.com/apache/flink/blob/7ba97419d200adef672b67c52c2b5a79f277cfda/flink-runtime/src/main/java/org/apache/flink/runtime/taskmanager/Task.java#L801


TaskManager will shutdown if `postFailureCleanUpRegistry.close();` meets any expetion, it works as expected to avoid poential bugs.


# Root cause and solution

The following unexpected exception occurred within `postFailureCleanUpRegistry.close();`.

Cause: The shutdown sequence is incorrect. `RemoteInputChannel` depends on `ChannelStatePersister` (which uses `ChannelStateWriteRequestExecutorImpl`) to snapshot buffers. However, `ChannelStateWriteRequestExecutorImpl` is being closed prematurely while `RemoteInputChannel` is still active. If a new buffer arrives, the active `RemoteInputChannel` attempts to use the already-closed executor, resulting in the failure.

Solution: Ensure the RemoteInputChannel (or remote components) is closed before ChannelStatePersister and ChannelStateWriteRequestExecutorImpl.

Core stack: 

```
Caused by: java.lang.IllegalStateException: not running
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorImpl.ensureRunning(ChannelStateWriteRequestExecutorImpl.java:349) ~
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorImpl.submitInternal(ChannelStateWriteRequestExecutorImpl.java:340) ~
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorImpl.submit(ChannelStateWriteRequestExecutorImpl.java:250) ~
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl.enqueue(ChannelStateWriterImpl.java:284) ~
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl.addInputData(ChannelStateWriterImpl.java:194) ~
	at org.apache.flink.runtime.io.network.partition.consumer.ChannelStatePersister.maybePersist(ChannelStatePersister.java:112) ~
	at org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel.onBuffer(RemoteInputChannel.java:623) ~
```

Full stack:

```
241672 [7c150f0de176c52cc390e6d10dd12c8d] Task [failing-map (1/3)#0] ERROR FATAL - exception in exception handler of task failing-map (1/3)#0 (e6367521ffc2df19786f2f8c034c0262_b8c789ec3a44294cb45da029ffe0e6fd_0_0).
java.lang.RuntimeException: java.io.IOException: java.lang.RuntimeException: unable to send request to worker
	at org.apache.flink.util.ExceptionUtils.rethrow(ExceptionUtils.java:321) ~
	at org.apache.flink.runtime.io.network.partition.consumer.BufferManager.recycle(BufferManager.java:237) ~
	at org.apache.flink.runtime.io.network.buffer.NetworkBuffer.deallocate(NetworkBuffer.java:189) ~
	at org.apache.flink.shaded.netty4.io.netty.buffer.AbstractReferenceCountedByteBuf.handleRelease(AbstractReferenceCountedByteBuf.java:111) ~
	at org.apache.flink.shaded.netty4.io.netty.buffer.AbstractReferenceCountedByteBuf.release(AbstractReferenceCountedByteBuf.java:101) ~
	at org.apache.flink.runtime.io.network.buffer.NetworkBuffer.recycleBuffer(NetworkBuffer.java:161) ~
	at org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer.clear(SpillingAdaptiveSpanningRecordDeserializer.java:140) ~
	at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.releaseDeserializer(AbstractStreamTaskNetworkInput.java:328) ~
	at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.close(AbstractStreamTaskNetworkInput.java:320) ~
	at org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput.close(StreamTaskNetworkInput.java:142) ~
	at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.close(StreamOneInputProcessor.java:88) ~
	at org.apache.flink.streaming.runtime.tasks.StreamTask.cleanUpInternal(StreamTask.java:1110) ~
	at org.apache.flink.util.IOUtils.closeAll(IOUtils.java:257) ~
	at org.apache.flink.core.fs.AutoCloseableRegistry.doClose(AutoCloseableRegistry.java:83) ~
	at org.apache.flink.util.AbstractAutoCloseableRegistry.close(AbstractAutoCloseableRegistry.java:127) ~
	at org.apache.flink.streaming.runtime.tasks.StreamTask.cleanUp(StreamTask.java:1101) ~
	at org.apache.flink.runtime.taskmanager.Task.lambda$restoreAndInvoke$2(Task.java:958) ~
	at org.apache.flink.runtime.taskmanager.Task.runWithSystemExitMonitoring(Task.java:973) ~
	at org.apache.flink.runtime.taskmanager.Task.lambda$restoreAndInvoke$3(Task.java:958) ~
	at org.apache.flink.util.IOUtils.closeAll(IOUtils.java:257) ~
	at org.apache.flink.core.fs.AutoCloseableRegistry.doClose(AutoCloseableRegistry.java:83) ~
	at org.apache.flink.util.AbstractAutoCloseableRegistry.close(AbstractAutoCloseableRegistry.java:127) ~
	at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:794) 
	at org.apache.flink.runtime.taskmanager.Task.run(Task.java:569) 
	at java.base/java.lang.Thread.run(Thread.java:829) [?:?]
Caused by: java.io.IOException: java.lang.RuntimeException: unable to send request to worker
	at org.apache.flink.runtime.io.network.partition.consumer.InputChannel.checkError(InputChannel.java:275) ~
	at org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel.checkPartitionRequestQueueInitialized(RemoteInputChannel.java:883) ~
	at org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel.notifyCreditAvailable(RemoteInputChannel.java:375) ~
	at org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel.notifyBufferAvailable(RemoteInputChannel.java:430) ~
	at org.apache.flink.runtime.io.network.partition.consumer.BufferManager.recycle(BufferManager.java:235) ~
	... 23 more
Caused by: java.lang.RuntimeException: unable to send request to worker
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl.enqueue(ChannelStateWriterImpl.java:287) ~
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl.addInputData(ChannelStateWriterImpl.java:194) ~
	at org.apache.flink.runtime.io.network.partition.consumer.ChannelStatePersister.maybePersist(ChannelStatePersister.java:112) ~
	at org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel.onBuffer(RemoteInputChannel.java:623) ~
	at org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler.decodeBufferOrEvent(CreditBasedPartitionRequestClientHandler.java:404) ~
	at org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler.decodeMsg(CreditBasedPartitionRequestClientHandler.java:297) ~
	at org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler.channelRead(CreditBasedPartitionRequestClientHandler.java:197) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:444) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:420) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:412) ~
	at org.apache.flink.runtime.io.network.netty.NettyMessageClientDecoderDelegate.channelRead(NettyMessageClientDecoderDelegate.java:112) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:444) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:420) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:412) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.DefaultChannelPipeline$HeadContext.channelRead(DefaultChannelPipeline.java:1410) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:440) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:420) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.DefaultChannelPipeline.fireChannelRead(DefaultChannelPipeline.java:919) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.epoll.AbstractEpollStreamChannel$EpollStreamUnsafe.epollInReady(AbstractEpollStreamChannel.java:800) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoop.processReady(EpollEventLoop.java:509) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoop.run(EpollEventLoop.java:407) ~
	at org.apache.flink.shaded.netty4.io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:997) ~
	at org.apache.flink.shaded.netty4.io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74) ~
	... 1 more
Caused by: java.lang.IllegalStateException: not running
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorImpl.ensureRunning(ChannelStateWriteRequestExecutorImpl.java:349) ~
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorImpl.submitInternal(ChannelStateWriteRequestExecutorImpl.java:340) ~
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorImpl.submit(ChannelStateWriteRequestExecutorImpl.java:250) ~
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl.enqueue(ChannelStateWriterImpl.java:284) ~
	at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl.addInputData(ChannelStateWriterImpl.java:194) ~
	at org.apache.flink.runtime.io.network.partition.consumer.ChannelStatePersister.maybePersist(ChannelStatePersister.java:112) ~
	at org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel.onBuffer(RemoteInputChannel.java:623) ~
	at org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler.decodeBufferOrEvent(CreditBasedPartitionRequestClientHandler.java:404) ~
	at org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler.decodeMsg(CreditBasedPartitionRequestClientHandler.java:297) ~
	at org.apache.flink.runtime.io.network.netty.CreditBasedPartitionRequestClientHandler.channelRead(CreditBasedPartitionRequestClientHandler.java:197) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:444) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:420) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:412) ~
	at org.apache.flink.runtime.io.network.netty.NettyMessageClientDecoderDelegate.channelRead(NettyMessageClientDecoderDelegate.java:112) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:444) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:420) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:412) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.DefaultChannelPipeline$HeadContext.channelRead(DefaultChannelPipeline.java:1410) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:440) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.AbstractChannelHandlerContext.invokeChannelRead(AbstractChannelHandlerContext.java:420) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.DefaultChannelPipeline.fireChannelRead(DefaultChannelPipeline.java:919) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.epoll.AbstractEpollStreamChannel$EpollStreamUnsafe.epollInReady(AbstractEpollStreamChannel.java:800) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoop.processReady(EpollEventLoop.java:509) ~
	at org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoop.run(EpollEventLoop.java:407) ~
	at org.apache.flink.shaded.netty4.io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:997) ~
	at org.apache.flink.shaded.netty4.io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74) ~
	... 1 more
```






"failing-map (1/12)#0@15867" prio=5 tid=0x4bb nid=NA runnable
  java.lang.Thread.State: RUNNABLE
	  at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorImpl.releaseSubtask(ChannelStateWriteRequestExecutorImpl.java:397)
	  at org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl.close(ChannelStateWriterImpl.java:274)
	  at org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.cancel(SubtaskCheckpointCoordinatorImpl.java:582)
	  at org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinatorImpl.close(SubtaskCheckpointCoordinatorImpl.java:569)
	  at org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$1408.664674019.close(Unknown Source:-1)
	  at org.apache.flink.util.IOUtils.closeAll(IOUtils.java:257)
	  at org.apache.flink.core.fs.AutoCloseableRegistry.doClose(AutoCloseableRegistry.java:83)
	  at org.apache.flink.util.AbstractAutoCloseableRegistry.close(AbstractAutoCloseableRegistry.java:127)
	  at org.apache.flink.streaming.runtime.tasks.StreamTask.cleanUp(StreamTask.java:1101)
	  at org.apache.flink.runtime.taskmanager.Task.lambda$restoreAndInvoke$2(Task.java:958)
	  at org.apache.flink.runtime.taskmanager.Task$$Lambda$2017.675782682.run(Unknown Source:-1)
	  at org.apache.flink.runtime.taskmanager.Task.runWithSystemExitMonitoring(Task.java:973)
	  at org.apache.flink.runtime.taskmanager.Task.lambda$restoreAndInvoke$3(Task.java:958)
	  at org.apache.flink.runtime.taskmanager.Task$$Lambda$2012.705621010.close(Unknown Source:-1)
	  at org.apache.flink.util.IOUtils.closeAll(IOUtils.java:257)
	  at org.apache.flink.core.fs.AutoCloseableRegistry.doClose(AutoCloseableRegistry.java:83)
	  at org.apache.flink.util.AbstractAutoCloseableRegistry.close(AbstractAutoCloseableRegistry.java:127)
	  at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:794)
	  at org.apache.flink.runtime.taskmanager.Task.run(Task.java:569)
	  at java.lang.Thread.run(Thread.java:829)


"failing-map (1/12)#0@15867" prio=5 tid=0x4bb nid=NA runnable
  java.lang.Thread.State: RUNNABLE
	  at org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate.close(SingleInputGate.java:733)
	  at org.apache.flink.runtime.taskmanager.InputGateWithMetrics.close(InputGateWithMetrics.java:150)
	  at org.apache.flink.runtime.taskmanager.Task.closeAllInputGates(Task.java:1048)
	  at org.apache.flink.runtime.taskmanager.Task.releaseResources(Task.java:1013)
	  at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:813)
	  at org.apache.flink.runtime.taskmanager.Task.run(Task.java:569)
	  at java.lang.Thread.run(Thread.java:829)

