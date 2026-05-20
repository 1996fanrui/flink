# Open TODOs / follow-up considerations

记录已被讨论但暂未落地的设计点，按需后续再决定。

## 1. `RecoverableInputChannel` push 入口合一（已收敛，留作历史记录）

接口当前对外保持双入口：

- `void onRecoveredStateBuffer(Buffer buffer)` — push a data buffer / RecoveryCheckpointBarrier
- `void finishRecoveredBufferDelivery()` — flip `allDelivered=true` + unconditional `notifyChannelNonEmpty()`（不再 push sentinel，见 `fix_rounds/recovery_in_recovery_flag_unification.md §9.2` 的删除决定）

由于 `finishRecoveredBufferDelivery` 已经不携带任何 buffer/sentinel 参数（只是个"宣告 delivery 结束 + drain-end wake"信号），跟 `onRecoveredStateBuffer` 也不再有"buffer + finish=true/false" 这种共用签名的诱因；保留双入口最自然，不需要再讨论合一。

`fix_rounds/recovery_in_recovery_flag_unification.md §9.4` 现在让两个入口各自直接落地（不再共享 `deliverRecoveredInternal` helper），因为 `finishRecoveredBufferDelivery` 退化成无 buffer 入参，跟 `onRecoveredStateBuffer` 不再有共用形状。

## 2. SpillFileReader 实例化时机（已收敛）

`fix_rounds/recovery_in_recovery_flag_unification.md §9.4.1` 已经定了：filter 收尾在 channelIOExecutor runnable 内**同步**构造 SpillFileReader 实例 + 写 `recoveryCheckpointTrigger` 字段 + `setFinalDrainEnabled`，再 `bufferFilteringCompleteFuture.complete(null)`——保证下游 mail #A 跑 `requestPartitions` 时 trigger 字段稳态、channel 构造时拿到的 `finalDrainEnabled` 跟 trigger 类型严格一致。

## 3. `finishPhysicalRecoveredChannels` fresh-job fallback 在 per-channel future 方案下的语义

`per-channel upstreamReady future` 方案下，`finishPhysicalRecoveredChannels`（spillFile==null 时给每 channel 调 `finishRecoveredBufferDelivery`）仍然合法——sentinel push 也会先 await upstreamReady，等齐才入队、isInRecovery 才有机会翻 false。语义正确，但 fresh-job 场景下"等所有 channel 上游 ready 再 push sentinel" 比 master 路径慢——评估实测延迟、如果不可接受再考虑短路（fresh-job 下根本不 push sentinel、跟 cpDuringRecovery=false 路径对齐）。
