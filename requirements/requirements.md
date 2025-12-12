当前在运行 org.apache.flink.test.checkpointing.UnalignedCheckpointRescaleITCase，偶尔遇到 运行期间 test 失败了。

失败的原因是 org.apache.flink.util.FlinkExpectedException: The TaskExecutor is shutting down.

requirements/simplified.log 是部分的日志，看起来 ChannelStateWriteRequestExecutorImpl throw java.lang.IllegalStateException: not running 导致 taskmanager.Task.doRun 在调用 postFailureCleanUpRegistry.close(); 时抛异常了。从而导致 TaskManager 主动退出了。

请分析这是不是 flink job 退出时的时序问题导致的 bug。分析并给出 root cause 和解决方案。
