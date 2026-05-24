# Open TODOs / follow-up considerations

记录已被讨论但暂未落地的设计点，按需后续再决定。

## 1. `RecoverableInputChannel` push 入口合一

当前接口对外有两个 push 方法（语义清晰但实现高度重叠）：

- `void onRecoveredStateBuffer(Buffer buffer)` — push a data buffer / barrier
- `void finishRecoveredBufferDelivery() throws IOException` — push end-of-stream sentinel + flip `allDelivered=true`

`fix_rounds/recovery_in_recovery_flag_unification.md §9.4` 已经把内部实现合并为 `deliverRecoveredInternal(Buffer buffer, boolean finish)`，两个对外入口都 delegate 给它。可能的进一步简化：把对外入口也合并成一个 `deliverRecovered(Buffer buffer, boolean finish)`。

**目前决定保留双入口**，理由：
- 合一会迫使 caller 自己构造 `EndOfInputChannelStateEvent` sentinel buffer——把 sentinel 类型泄漏给 SpillFileReader / fresh-job fallback / Step 1 等所有 caller，调用 site 不再"声明语义"而是"摆弄实现细节"
- 双入口语义读者一眼就能区分"我推数据" vs "我宣告 delivery 结束"，比 `(buffer, true/false)` 更显式
- 内部实现合并已经达到代码去重目标，外部接口大小不再是收益的关键

**何时回头考虑合一**：如果未来出现第三种 push 入口、或者 caller 普遍都要 import sentinel 类型（场景演变让"声明语义"变成噪声），再统一为单入口；目前没必要。

## 2. SpillFileReader 实例化时机

`fix_rounds/recovery_in_recovery_flag_unification.md §4.2` 留了一个小 trade-off：SpillFileReader 实例化可以放在 filter 收尾时（早；trigger 字段一并写好）或推迟到 drain handoff 时（晚；channel 已 publish 才绑定）。两种都对，差别在初始化的微秒级时序，未落地代码前暂留意。

## 3. `finishPhysicalRecoveredChannels` fresh-job fallback 在 per-channel future 方案下的语义

`per-channel upstreamReady future` 方案下，`finishPhysicalRecoveredChannels`（spillFile==null 时给每 channel 调 `finishRecoveredBufferDelivery`）仍然合法——sentinel push 也会先 await upstreamReady，等齐才入队、isInRecovery 才有机会翻 false。语义正确，但 fresh-job 场景下"等所有 channel 上游 ready 再 push sentinel" 比 master 路径慢——评估实测延迟、如果不可接受再考虑短路（fresh-job 下根本不 push sentinel、跟 cpDuringRecovery=false 路径对齐）。
