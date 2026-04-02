# 验收方案

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-HT7T | 锁外执行 channel 转换与资源释放 | REQ-GDU7 | 通过 | Agent 执行 | grep 确认 toInputChannel/releaseAllResources/getBuffersInUseCount 均在 synchronized 块之前，synchronized 块仅含数据结构更新 |
| AT-21IC | 现有单元测试回归验证 | REQ-GDU7 | 通过 | 代码自动化 | mvn test SingleInputGateTest(31)+RecoveredInputChannelTest(3)=34 tests, 0 failures |
| AT-3BKF | 锁顺序注释已删除 | REQ-V9VD | 通过 | Agent 执行 | grep -c "Lock ordering note" 输出为 0 |

---

### [L2-Agent] AT-HT7T 锁外执行 channel 转换与资源释放

**目标**：验证 `convertRecoveredInputChannels()` 方法中 `toInputChannel()`、`releaseAllResources()`、`getBuffersInUseCount()` 均在 `synchronized(inputChannelsWithData)` 块之外执行。

**操作步骤**：

1. 读取 `SingleInputGate.java` 中 `convertRecoveredInputChannels()` 方法的完整代码
2. 验证以下条件全部满足：
   - `toInputChannel()` 调用不在任何 `synchronized(inputChannelsWithData)` 块内
   - `releaseAllResources()` 调用不在任何 `synchronized(inputChannelsWithData)` 块内
   - `getBuffersInUseCount()` 调用不在任何 `synchronized(inputChannelsWithData)` 块内
   - `synchronized(inputChannelsWithData)` 块仅包含数据结构更新操作（队列移除/添加、map 更新、数组更新）
3. 验证方法上不存在包含 "Lock ordering note" 的 Javadoc 注释

**预期结果**：所有可能获取 `receivedBuffers` 锁的方法调用均在 `inputChannelsWithData` 锁之外，消除锁顺序反转。

**判定命令**：
```bash
# 提取 convertRecoveredInputChannels 方法体，检查 synchronized 块内不包含 toInputChannel/releaseAllResources/getBuffersInUseCount
grep -A 100 'public void convertRecoveredInputChannels' flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/SingleInputGate.java | head -80
```

**客观证据**：grep 输出显示 `toInputChannel()`、`releaseAllResources()`、`getBuffersInUseCount()` 在 `synchronized` 块之前。

---

### [L1-测试] AT-21IC 现有单元测试回归验证

**目标**：确保重构未破坏现有功能。

**操作步骤**：

运行 `SingleInputGate` 和 `RecoveredInputChannel` 相关的现有测试。

**命令**：
```bash
cd /Users/ruifan/code/github/flink-os-2 && mvn test -pl flink-runtime -Dtest="SingleInputGateTest,InputGateTest,RecoveredInputChannelTest,LocalRecoveredInputChannelTest,RemoteRecoveredInputChannelTest" -DfailIfNoTests=false
```

**预期结果**：所有测试通过，退出码为 0。

---

### [L2-Agent] AT-3BKF 锁顺序注释已删除

**目标**：验证 `convertRecoveredInputChannels()` 方法上的 "Lock ordering note" Javadoc 已被删除。

**操作步骤**：

1. 在 `SingleInputGate.java` 中搜索 "Lock ordering note"
2. 确认该文本不存在

**判定命令**：
```bash
grep -c "Lock ordering note" flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/SingleInputGate.java
```

**预期结果**：输出为 `0`，表示该注释已删除。

**客观证据**：grep 计数为 0。
