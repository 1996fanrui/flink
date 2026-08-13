# Roman (rkhachatryan) 对 PR #28662 的全部评论

PR: [FLINK-40080][checkpoint] Drain spilled state into channels; replay spilled slices into checkpoints
（https://github.com/apache/flink/pull/28662）

共 5 条行内评论 + 1 条 review 总评，全部来自 2026-08-12 22:51–22:58 UTC 的同一轮 review（`COMMENTED`，非 request-changes）。
总评原话："Mostly LGTM, I have only minor comments + 1 potential buffer leak."
原始评论保留英文原文；分析与结论用中文。

状态标注：✅ 已 resolve / 🕓 已处理，待他确认 / ⏳ 待我们回复

## 总览

**状态（2026-08-13）**：6 条代码改动全部完成，**回复一条都还没发**；#6 还等人工建 JIRA 拿编号。

| # | 位置 | 主题 | 状态 | 处理结果 |
|---|------|------|------|----------|
| 1 | FetchedChannelStateDrainer.java:125 | 出错时 buffer 未回收 | ⏳ 待我们回复 | ✅ 已改 + 新单测 |
| 2 | FetchedChannelStateDrainer.java:135 | 申请 buffer 前先判 EOF | ⏳ 待我们回复 | ✅ 已改 |
| 3 | FetchedChannelStateDrainer.java:161 | `remaining` 恒正 → 应是前置条件 | ⏳ 待我们回复 | ✅ 已改 |
| 4 | ChannelStateCheckpointWriter.java:171 | `catch (Exception ignored)` 应打日志 | ⏳ 待我们回复 | ✅ 已改 |
| 5 | ChannelStateWriter.java:241（NO_OP） | 同上 | ⏳ 待我们回复 | ✅ 已改 |
| 6 | review 总评 | 新代码路径完全没有日志 | ⏳ 待我们回复 | ⏸ 等人工建 JIRA（草稿见 #6） |

## 回复语一览（直接复制）

| # | 回复 |
|---|------|
| 1 | Good catch, fixed + a test. The reference is dropped *before* `onRecoveredStateBuffer`, which takes ownership even when it throws, so a delivered buffer is never recycled twice. |
| 2 | Good point, that allocation is gone now. No EOF probe needed though: the segment length is known up front, so the loop stops before requesting a buffer it would immediately recycle. |
| 3 | Right, it can never be 0 at the call site — `checkArgument` now. The javadoc's EOF wording was wrong too: netty returns -1. |
| 4 | Done. |
| 5 | Done (added a logger to `NoOpChannelStateWriter` for it). |
| 6 | Agreed, the new path is silent today — filed \<FLINK-XXXXX\> for it (feature-enabled at INFO plus the fetch/filter/drain transitions) and will do it in a separate PR so this one doesn't grow. |

## 实际改动（2026-08-13）

| 文件 | 改动 | 对应 |
|---|---|---|
| `FetchedChannelStateDrainer#drainSegment` | 包 `try/catch (Throwable)`，只回收尚未交接的 buffer；交接前先置空引用 | #1 |
| `FetchedChannelStateDrainer#drainSegment` | 加 `int remaining = seg.length()` 计数器，段在 buffer 边界读完就 `return` | #2 |
| `FetchedChannelStateDrainer#fill` | `if (remaining == 0) return 0;` → `checkArgument(remaining > 0)`；javadoc 修正 | #3 |
| `ChannelStateCheckpointWriter#writeInputFromSpill` | `catch (Exception ignored)` → `LOG.info(..., checkpointId, e)` | #4 |
| `ChannelStateWriter.NoOpChannelStateWriter` | 新增 logger，catch 同样改成 `LOG.info` | #5 |
| `FetchedChannelStateDrainerTest` | 新增 `testDrainRecyclesInFlightBufferWhenBodyReadFails`；`RecordingChannel` 加 `requested`/`recycled` 计数 | #1 |

**验证**：`mvn -o -pl flink-runtime -am test -Dtest='FetchedChannelState*Test,ChannelState*Test'` 全绿（`checkpoint.channel` 包下 ~154 个，`FetchedChannelStateDrainerTest` 14 个）；`spotless:check` BUILD SUCCESS。

**剩余待办**：人工建 FLINK JIRA（#6）→ 编号填进回复 6 → push fixup → 一次性 submit 6 条回复。

---

## 1. FetchedChannelStateDrainer.java:125 — 出错时 buffer 未回收

链接：https://github.com/apache/flink/pull/28662#discussion_r3770934211

> This buffer might not be released in case of error
>
> (ditto: 2nd allocation)

**成立，是这轮唯一的正确性问题。** `fill()` 抛 `IOException`（spill 文件读坏/截断）时，手上那块 buffer 没人回收；而 `MemorySegment` 只有 `recycleBuffer()` 才回 `NetworkBufferPool`，`LocalBufferPool` 销毁不替已发放的 buffer 回收，所以漏掉的段在 TM 生命周期内不再回来。

**关键陷阱**：`RecoverableInputChannel#onRecoveredStateBuffer` 的所有权是在**调用瞬间**转移的，不是"成功返回后"——`RecoveredInputChannel.java:139-160` 的 try/finally 保证它无论走哪条路（已 release 静默回收、入队成功、`notifyChannelNonEmpty()` 抛异常）都由 channel 负责这块 buffer。所以引用必须**在调用之前**置空，否则 `onRecoveredStateBuffer` 或紧随的 `seg.commit()` 一抛，catch 就会回收一块已入队的 buffer，正好踩回 FLINK-40345 那个 double-recycle。

**已改**：

```java
Buffer buf = ch.requestRecoveryBufferBlocking();
try {
    ...
    Buffer full = buf;
    buf = null;              // 先交出引用，再调用
    synchronized (lock) {
        ch.onRecoveredStateBuffer(full);
        seg.commit();
    }
    buf = ch.requestRecoveryBufferBlocking();
    ...
} catch (Throwable t) {
    if (buf != null) {
        buf.recycleBuffer();
    }
    throw t;
}
```

**测试**：`testDrainRecyclesInFlightBufferWhenBodyReadFails` —— 写 64 字节 body、把 spill 文件截掉 24 字节，前两块 16 字节正常交付，第三块填到 8 字节撞 `EOFException`；断言 `requested == 3`、`recovered == 2`、`recycled == 1`。

**回复**：Good catch, fixed + a test. The reference is dropped *before* `onRecoveredStateBuffer`, which takes ownership even when it throws, so a delivered buffer is never recycled twice.

---

## 2. FetchedChannelStateDrainer.java:135 — 申请 buffer 前先判 EOF

链接：https://github.com/apache/flink/pull/28662#discussion_r3770935338

> Is it possible to check for EOF before requesting a buffer?

**他说得对，那次申请确实是白费的**：段长度恰为 buffer 容量整数倍时，每个段都会多申请一块 buffer、读到 EOF、再回收；而 `requestRecoveryBufferBlocking()` 是阻塞的，buffer 紧张时等于为一块不需要的 buffer 去和别的 channel 抢。

**但不用探测 EOF**：`SpillSegment#length()` 已给出精确字节数。它是"**当前这个 view** 从头到尾会吐出的字节数"，构造时固定、读了不会减少（`BoundedSegmentStream.length` 是 final 字段）；resume 路径传的是 snapshot 那一刻尚未交付的剩余，所以对原始段而言才像"剩余量"。这里只拿它当初值，自己递减。

**已改**（人工 2026-08-13 拍板：结构不动，只加计数器）：

```java
int remaining = seg.length();
...
    remaining -= cap;
    交付;
    if (remaining == 0) {
        // The segment ends on this buffer boundary: no point in requesting a
        // buffer that the EOF below would immediately recycle.
        return;
    }
    buf = ch.requestRecoveryBufferBlocking();
```

`testDrainSegmentExactMultipleOfBufferHasNoPartialTail` 的注释跟着更新（不再是"申请后回收"，而是"根本不申请"），断言不变。

**回复**：Good point, that allocation is gone now. No EOF probe needed though: the segment length is known up front, so the loop stops before requesting a buffer it would immediately recycle.

---

## 3. FetchedChannelStateDrainer.java:161 — `remaining` 恒正，应写成前置条件

链接：https://github.com/apache/flink/pull/28662#discussion_r3770950468

> `remaining` here is the remaining `buf` size right?
> Which must always be positive - so it should be a precondition?

**成立，是死代码。** 唯一调用点 `fill(buf, in, cap - buf.getSize())`：新 buffer `getSize() == 0`，未满时 `cap - getSize() > 0`，填满即刻交付换新，所以到不了 0。

**已改**：换成 `checkArgument(remaining > 0)`；javadoc 补上"第三参是 buffer 的剩余可写空间"，并修掉 "returns 0 if the stream is at EOF"——底层 netty `ByteBuf.writeBytes(InputStream, int)` 在 EOF 返回 -1（`ChannelStateSerializer.java:76-95`），现改为 "or a non-positive value"。

**回复**：Right, it can never be 0 at the call site — `checkArgument` now. The javadoc's EOF wording was wrong too: netty returns -1.

---

## 4. ChannelStateCheckpointWriter.java:171 — 吞掉 `reader.close()` 的异常

链接：https://github.com/apache/flink/pull/28662#discussion_r3770954571
锚点：`writeInputFromSpill()` 的 `isDone()` 早退分支

> Log at info?

**合理**：close 失败不影响正确性（spill 文件是本地临时文件，最坏留个句柄），但静默吞掉会让排查文件句柄问题时无迹可循。本类已有 `LOG`。

**已改**：

```java
} catch (Exception e) {
    LOG.info("Failed to close the fetched channel state reader of checkpoint {}", checkpointId, e);
}
```

**回复**：Done.

---

## 5. ChannelStateWriter.java:241 — NO_OP 里同样的静默吞异常

链接：https://github.com/apache/flink/pull/28662#discussion_r3770957062
锚点：`NoOpChannelStateWriter#addInputDataFromSpill`

> Log at info?

**合理，同 #4。** 差别只在 `ChannelStateWriter` 接口里没有 logger。

**已改**：`NoOpChannelStateWriter` 加 `private static final Logger LOG = LoggerFactory.getLogger(NoOpChannelStateWriter.class);`，catch 块与 #4 同一句措辞。

**回复**：Done (added a logger to `NoOpChannelStateWriter` for it).

---

## 6. review 总评 — 新代码路径没有任何日志

链接：https://github.com/apache/flink/pull/28662#pullrequestreview-4921785962

> Apart from that, I realized that the new code path never logs that it's enabled; nor the state transitions (fetching from s3 -> filtering -> draining etc).
>
> Could you add a ticket/PR to fix that?
> I think we should log in `StreamTask.recoverChannelsWithCheckpointing`:
> 1. The feature is enabled (INFO)
> 2. Some transitions in Futures chain (INFO or DEBUG)

**合理**：`StreamTask.recoverChannelsWithCheckpointing`（`StreamTask.java:951-995`）整条链一行日志都没有，线上无法判断走没走 CDR 路径、卡在哪一步。这是整个 CDR（checkpointing during recovery）的可观测性问题，spilling 只是其中一段。

**走单独 PR**，不塞进本 PR（本 PR 已 CI 绿、他也说 mostly LGTM）。JIRA 由人工创建，编号填进回复。

### JIRA 草稿（供人工创建）

- **Issue Type**: Improvement ｜ **Component/s**: Runtime / Checkpointing ｜ **Priority**: Minor
- **Parent / Related**: FLINK-40080（评论来源）；伞票挂 CDR 那张，不是 spilling 的 FLINK-38544

**Summary**：

```
Log the enablement and state transitions of checkpointing during recovery
```

**Description**：

```
StreamTask#recoverChannelsWithCheckpointing produces no log at all today: we cannot tell from
a TaskManager log whether checkpointing during recovery is enabled, nor how far the recovery
got.

Proposed:
* INFO when the feature is enabled.
* INFO/DEBUG on the transitions of the futures chain (fetching -> filtering -> draining).

Reported by Roman Khachatryan in the review of FLINK-40080:
https://github.com/apache/flink/pull/28662#pullrequestreview-4921785962
```

**回复**：Agreed, the new path is silent today — filed \<FLINK-XXXXX\> for it (feature-enabled at INFO plus the fetch/filter/drain transitions) and will do it in a separate PR so this one doesn't grow.
