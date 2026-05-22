# Round 3 — `rui_tools/loop.sh` 第一次验证结果

Log: `log/20260522_173914.log`

## 上一轮 fix（filter+drain 单次 submit）已生效

整轮日志**没有任何 `RejectedExecutionException`**，原始 round2 的根因被彻底消除。

但新跑暴露出另一个**之前被 round2 bug 掩盖**的故障：drain 真正跑起来后，第一条 entry 就在 `SpillFileReader.drain` 里直接抛 `IllegalStateException`，导致 rescale 类 ITCase 仍然 FAILED。

## 新故障 — 同一根因，多次出现

所有失败统一来自 rescale 路径上的 recovery task（`upscale0` / `global0` / `downscale0` / `rebalance0` 等 `channel-state-unspilling-*` task），堆栈一致：

```
org.apache.flink.runtime.taskmanager.AsynchronousException: Unable to drain recovered channel state
  at StreamTask$StreamTaskAsyncExceptionHandler.handleAsyncException(StreamTask.java:1910)
  at StreamTask.lambda$restoreStateAndGates$6(StreamTask.java:949)
Caused by: java.lang.IllegalStateException:
  Drain: no physical channel found for InputChannelInfo{gateIdx=0, inputChannelIdx=3}
  at SpillFileReader.drain(SpillFileReader.java:109)
```

每次失败的 `inputChannelIdx` 不同（看到过 2 / 3 / 4），但 `gateIdx=0`，都发生在 **upscale / downscale / rebalance** 这种**改变并行度**的场景。非 rescale 的 `UnalignedCheckpointITCase`（11/11） 与 `UnalignedCheckpointRescaleWithMixedExchangesITCase` 在 round2 log 中是全过的，这一次也未观察到 drain 相关失败。

## 根因 — filter 用 OLD InputChannelInfo 写 spillFile，drain 用 NEW InputChannelInfo 查

`RecoveredChannelStateHandler.recover(channelInfo, oldSubtaskIndex, ...)` 在 rescale 路径下被反复调用：

1. **入参 `channelInfo` 是 OLD topology 的 InputChannelInfo**——它来自 checkpoint 元数据里 per-channel 的 key（rescale 之前的并行度划分）。
2. 内部调用 `RecoveredInputChannel channel = getMappedChannels(channelInfo)`，`getMappedChannels` → `calculateMapping` 通过 `RescaleMappings.invert()` 把 OLD channel index 映射成 NEW channel index，再 `getChannel(gateIdx, mappedIndexes[0])` 拿到**当前（NEW）topology** 的 RecoveredInputChannel。
3. 然后 `recoverWithFiltering(channel, channelInfo, oldSubtaskIndex, ...)` 一行：

   ```java
   accumulator.beginChannel(channelInfo);   // ← BUG：用了 OLD info
   ```

   `accumulator.beginChannel` 这个 InputChannelInfo 会被原样写到 `SpillFile.append(currentChannel, payload)` 里，**成为 spillFile entry 的 key**。
4. drain 阶段 `SpillFileReader` 是用 `physicalChannels = collectPhysicalChannels(inputGates)` 构造 `channelByInfo`，key 是**转换后**物理 channel 的 `getChannelInfo()`——这一定是 NEW topology 的 InputChannelInfo（`InputChannel` 构造器里 `channelInfo = new InputChannelInfo(gateIdx, channelIndex)`，channelIndex 是 NEW topology 的 index）。
5. drain 用 OLD info 在 NEW info 的 map 里查找 → `null` → `IllegalStateException`。

非 rescale 路径下 OLD == NEW，所以 `UnalignedCheckpointITCase` 不暴露这个 bug；rescale 路径 OLD ≠ NEW，必然踩中。

## 关键代码引用

`RecoveredChannelStateHandler.java:243`：
```java
accumulator.beginChannel(channelInfo);  // OLD info
```

`SpillFileReader.java:107-111`：
```java
RecoverableInputChannel ch = channelByInfo.get(e.channelInfo);  // 用 OLD 查 NEW
if (ch == null) {
    throw new IllegalStateException(
            "Drain: no physical channel found for " + e.channelInfo);
}
```

`RecoveredChannelStateHandler.java:374-389` （映射逻辑，证明 channel 已是 NEW）：
```java
private RecoveredInputChannel getMappedChannels(InputChannelInfo channelInfo) {
    return rescaledChannels.computeIfAbsent(channelInfo, this::calculateMapping);
}
private RecoveredInputChannel calculateMapping(InputChannelInfo info) {
    int[] mappedIndexes = oldToNewMapping.getMappedIndexes(info.getInputChannelIdx());
    return getChannel(info.getGateIdx(), mappedIndexes[0]);
}
```

## 修复方向

`accumulator.beginChannel` 用 NEW info 即可：

```java
accumulator.beginChannel(channel.getChannelInfo());   // NEW info（来自映射后的 channel 本身）
```

`filterAndRewrite` 仍然继续用 OLD `channelInfo.getInputChannelIdx()` —— 它需要 OLD channel index 才能在 `GateFilterHandler` 里找到对应的 OLD 虚拟通道做过滤，这一段不能动。

改动只 1 行。文件归属：`RecoveredChannelStateHandler.java` 属于 **Phase 3**（filter + spillFile 写入）。会以 `[FLINK-38544][fix][phase3]` 单独 commit 出现。

## 验证计划

1. clean install + loop.sh 跑一次：rescale ITCase 必须 0 failure；
2. 通过后，按之前商定的"一次成功 → 再跑 5 次"流程继续验证。

## 待你确认

1. 上面"OLD InputChannelInfo 写 spillFile / NEW InputChannelInfo 查物理 channel"的根因诊断是否正确？
2. 1 行修复方向（`beginChannel(channel.getChannelInfo())`）能不能采纳？
3. 同意后我直接进入 `[fix][phase3]` 修复 + verify 流程。
