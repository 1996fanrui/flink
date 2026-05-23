# JVM 测试卡死排查手册（for agents）

适用场景：JUnit/surefire/Maven 启动的 JVM 测试跑着跑着不动了；进程还在，CPU 接近 0，日志不再增长。

## 0. 核心原则

- **永远不要直接 `wait` 等卡住的进程结束**。它不会自己结束。
- 先看 thread dump（便宜，几秒钟出全图），再决定要不要 heap dump（贵，可能 GB 级）。
- thread dump 给你**线程都阻塞在哪行代码**；heap dump 给你**对象字段的具体值**。两者互补。

## 1. 启动测试 + 自动停掉

不要前台 blocking 等待。后台跑 + 监控日志增长 + 60 秒不增长就判定 stall。

```bash
# 后台启动 maven 测试（loop.sh 内部封装了 mvn 命令）
bash rui_tools/loop.sh > /tmp/loop.log 2>&1 &
```

然后用 Monitor 工具或下面这段 polling 脚本，**循环检测最新日志文件 60 秒不增长就退出**：

```bash
LOG=$(ls -t log/2026*.log | head -1)
last=0; stall=0
while true; do
  jpid=$(pgrep -f "surefirebooter-.*\.jar" | head -1)
  [ -z "$jpid" ] && break
  cur=$(wc -c < "$LOG")
  if [ "$cur" = "$last" ]; then
    stall=$((stall+5))
    [ $stall -ge 60 ] && { echo "STALL pid=$jpid log=$LOG"; break; }
  else stall=0; last=$cur; fi
  sleep 5
done
```

关键点：
- `pgrep -f "surefirebooter-.*\.jar"` 找**真正的 JVM 进程**（不是 `/bin/sh` 包装、不是 mvnw wrapper）。
- 监控的是**日志大小**（`wc -c`），不是 mtime；mtime 可能因系统事件抖动。

## 2. Thread dump（先做这步）

```bash
JPID=$(ps aux | grep -E "surefirebooter-.*\.jar" | grep -v "/bin/sh" | awk '{print $2}' | head -1)
jstack -l $JPID > /tmp/dump1.txt
sleep 5
jstack -l $JPID > /tmp/dump2.txt  # 第二份，对比验证是不是真死锁
```

分析：
- `grep "java.lang.Thread.State:" /tmp/dump1.txt | sort | uniq -c` — 一眼看出多少线程在 RUNNABLE/WAITING/PARKING。
- 找你关心的业务线程，看 stack 最顶端阻塞在哪个 method/lock object id。
- diff 两份 dump（剔除 cpu/elapsed 等变化字段）→ 业务线程 stack 完全一致 = **真死锁，不是慢**。

```bash
sed -E 's/cpu=[0-9.]+ms //; s/elapsed=[0-9.]+s //' /tmp/dump1.txt > /tmp/d1
sed -E 's/cpu=[0-9.]+ms //; s/elapsed=[0-9.]+s //' /tmp/dump2.txt > /tmp/d2
diff /tmp/d1 /tmp/d2 | head -50
```

## 3. Heap dump（thread dump 不够用时再做）

什么时候需要：thread 看出"在等某个 future 完成"，但 future 是哪个对象、对象字段是什么值——这需要 heap。

```bash
mkdir -p /tmp/agent-tmp/heap
HEAP=/tmp/agent-tmp/heap/dump_$(date +%H%M%S).hprof
jmap -dump:format=b,file=$HEAP $JPID
```

macOS 注意：`jmap` 经常因为 `task_for_pid` 权限失败。如果失败，确认 JPID 是真正的 java 进程而不是 `/bin/sh` PID。

## 4. 解析 hprof — **必须用 Eclipse MAT**

不允许自己写 Python parser、不允许用 jhat / jhsdb、不允许靠 `strings` 凑。**强制 MAT**。

### 4.1 安装（如果缺）

```bash
# 检测是否已装
ls /Applications/MemoryAnalyzer.app/Contents/Eclipse/ParseHeapDump.sh 2>/dev/null \
  || brew install --cask memoryanalyzer
```

CLI 入口：`/Applications/MemoryAnalyzer.app/Contents/Eclipse/ParseHeapDump.sh`

### 4.2 第一次解析（建索引）

```bash
HEAP=/tmp/agent-tmp/heap/dump.hprof
/Applications/MemoryAnalyzer.app/Contents/Eclipse/ParseHeapDump.sh "$HEAP"
```

这会在 hprof 同目录生成 `*.index` 系列文件，后续查询会复用，不会重新扫一遍 hprof。

### 4.3 跑内置 leak suspects 报告（快速概览）

```bash
/Applications/MemoryAnalyzer.app/Contents/Eclipse/ParseHeapDump.sh "$HEAP" \
  org.eclipse.mat.api:suspects
```

产物 `*_Leak_Suspects.zip`，解压后 `index.html` 有 GC root、最大对象、dominator 视图。

### 4.4 精确查字段值 / 引用关系 — 用 GUI

```bash
open /Applications/MemoryAnalyzer.app "$HEAP"
```

打开后在 **OQL** 视图执行查询，例如：

```sql
-- 查所有 LocalInputChannel 的关键字段
SELECT
    s.allRecoveredBuffersDelivered,
    s.hasPendingPriorityEvent,
    s.recoverySequenceNumber,
    s.subpartitionView,
    s.recoveredBuffers
FROM org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel s

-- 查某个对象的所有 inbound 引用（谁持有这个对象）
SELECT * FROM INBOUNDS(0x64df09da0)
```

GUI 同时支持右键 → "List objects" → "with incoming references" / "with outgoing references"，比 OQL 更直观。

### 4.5 关键 OQL 模板

| 目的 | OQL |
|---|---|
| 列出 class 所有实例的指定字段 | `SELECT s.field1, s.field2 FROM com.example.MyClass s` |
| 找未完成的 CompletableFuture | `SELECT s FROM java.util.concurrent.CompletableFuture s WHERE s.result = null` |
| 找 BitSet 内部 long 数组的实际值 | 双击 BitSet 实例 → `words` 字段 → 看 `long[]` |
| 找某 future 被谁持有 | 右键 future 实例 → Path to GC Roots |

## 5. 排查的思考顺序

1. **thread dump → 找阻塞线程的 stack**。比如 `mailbox.take`、`CompletableFuture.get`、`Object.wait`。
2. **从 stack 推断它在等什么**（哪个 future / 哪个条件变量 / 哪个 lock）。
3. **如果是等 future**：去 heap 找那个 future 对象，看 `result` 字段——如果还是 null，说明 future 永远不会完成。
4. **如果是等条件变量**：找谁会 `notify`/`signal` 那个对象，是不是某个唤醒路径漏了。
5. **持续验证状态稳定**：隔 5-10 秒再抓一份 dump，对比两份的 stack 与字段是否完全一致——一致才说明真死锁。
6. **不要相信你脑补的事件序列**。heap 只能证明"卡死时刻的状态"，不能证明"如何走到这个状态"。要证明事件顺序，得加 log + 重跑。

## 5.1 Flink 消费/checkpoint 卡死专项流程

这是 §5 的专项展开，针对「下游 sink/operator 不再消费、上游有数据堆着」这类 hang。**严格按顺序、不要跳步**。

### Step 1: 定位「应该消费但卡住的 InputChannel」

从 thread dump 找到所有阻塞在 mailbox 的 operator 线程，对每个对应的 `SingleInputGate` 检查：

```sql
SELECT g.@objectAddress AS gate, g.gateIndex, g.requestedPartitionsFlag,
       g.hasReceivedAllEndOfPartitionEvents,
       g.enqueuedInputChannelsWithData,
       g.inputChannelsWithData,
       g.availabilityHelper.availableFuture.result AS availFut
FROM org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate g
```

**判定"卡住"的硬性条件**（同时成立）：
- `requestedPartitionsFlag = true`（已经请求过 partitions，进入消费阶段）
- `hasReceivedAllEndOfPartitionEvents = false`（还没收到所有 EOP，逻辑上还该消费）
- `availFut = null`（availability future 未完成，consumer 在等待）
- `enqueuedInputChannelsWithData` 的 BitSet `words[0] = 0x0`（没有任何 channel 在 queue 里）

四个全中 = **gate 在等数据，但它认为自己没数据可拉**——这就是要排查的 gate。

### Step 2: 找出该 gate 下"应该有数据"的 channel

枚举 `channels` 数组里的每个 `InputChannel`，按类型分别查它的"有数据的迹象"：

```sql
-- LocalInputChannel：查 recovery 队列 / view 引用
SELECT c.@objectAddress, c.channelInfo.gateIdx AS g, c.channelInfo.inputChannelIdx AS ci,
       c.allRecoveredBuffersDelivered AS delivered,
       c.recoveredBuffers AS recQ,
       c.subpartitionView AS view,
       c.hasPendingPriorityEvent AS pri
FROM org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel c

-- RemoteInputChannel：查 receivedBuffers + recoveredBuffers
SELECT c.@objectAddress, c.channelInfo.gateIdx AS g, c.channelInfo.inputChannelIdx AS ci,
       c.allRecoveredBuffersDelivered AS delivered,
       c.recoveredBuffers AS recQ,
       c.receivedBuffers AS recvQ
FROM org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel c
```

**"应该消费但被忽略"的判定**：
- `delivered = true` 且 `recQ.size = 0` 且 `recvQ.size = 0`（如果是 Remote）——本地真的没数据
- 但下面 Step 3 会发现上游有数据 → 矛盾 → 这个 channel 就是卡住的

### Step 3: 顺着 channel **直接找到对应的上游 subpartition**

**关键纪律**：必须通过引用关系找到**这个 channel 对应的那个 subpartition**，不能随便抓一个 `PipelinedSubpartition` 看。

- **LocalInputChannel**：`channel.subpartitionView.parent` 就是上游 subpartition。
  - 双向校验：`subpartition.readView == channel.subpartitionView` 必须成立。
- **RemoteInputChannel**：上游在远端 TM。本地 heap 拿不到。改去查 `receivedBuffers` 本身（netty push 后就在这里）。

OQL：
```sql
-- Local: 顺着 view 找到上游
SELECT c.@objectAddress AS chan,
       c.subpartitionView AS view,
       c.subpartitionView.parent AS upSub
FROM org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel c
```

GUI 里：右键 channel 实例 → "List objects" → "with outgoing references"，跟着 `subpartitionView` → `parent` 走。

### Step 4: 检查匹配上的 upstream subpartition 状态

对**Step 3 找到的那个 subpartition**（不是随便一个）查：

```sql
SELECT p.@objectAddress AS sub,
       p.readView,
       p.isFinished,
       p.isBlocked,
       p.flushRequested,
       p.buffers.numPriorityElements AS pri,
       p.totalNumberOfBuffers AS total,
       p.buffersInBacklog AS backlog
FROM org.apache.flink.runtime.io.network.partition.PipelinedSubpartition p
```

**异常判定矩阵**（结合业务期望）：

| 字段 | 异常组合 | 含义 |
|---|---|---|
| `readView = null` | 任意 | 下游没 subscribe 上来，通知链路从头就断了 |
| `readView != null` + `flushRequested = true` + `total > 0` | 下游 gate 不知道有数据 | **典型 race：source emit 时 view 未建或者后续 notify 漏了** |
| `isBlocked = true` | 业务没主动 block | **可疑**——只有 alignment 等明确语义会 block，没原因的 block 是 bug |
| `isFinished = true` + 下游还在等 | 永远等不到了 | upstream 已结束，但 EOP 没传递成功 |
| `pri > 0` | 下游 channel `hasPendingPriorityEvent = false` | priority 通知漏掉 |
| `total = 0` | 全空 | 上游真没数据；问题在上游再上游 |

### Step 5: 多 subpartition 时按"业务期望"对照

如果同一 task 的多个 subpartition 状态不一致（比如我们这次：3 个 `isBlocked=false buffers=14`、6 个 `isBlocked=true buffers=2`），**必须问清楚业务期望**：
- 这些 blocked 是 unaligned 模式下某些 edge 不支持回退到 aligned 导致的预期 block，还是没有理由的 block？
- 卡住的 channel 对应的 subpartition 是 blocked 还是 not blocked？只有那个才是 root cause 的关键。

不要把"看起来不正常的 subpartition"和"卡住 channel 对应的 subpartition"混为一谈。

### Step 6: 找通知漏掉的位置

确认了「下游 gate 不知道上游有数据」+「上游 readView 已建」之后，问题一定在以下一条通知链中的某一节漏了：

```
source emit → PipelinedSubpartition.add → notifyDataAvailable (if readView != null)
            → readView.notifyDataAvailable
            → channel.notifyDataAvailable(view)
            → notifyChannelNonEmpty
            → gate.notifyChannelNonEmpty(channel)
            → queueChannel → queueChannelUnsafe
            → 如果 size 变 1，notification.notifyDataAvailable() 完成 availableFuture
```

可能的漏点：
- `subpartition.add` 时 `readView == null`（下游还没 subscribe）→ 通知丢；后续 add 因 `flushRequested == true` 不再 notify
- `queueChannelUnsafe` 因 `channelsWithEndOfPartitionEvents` bit 已设、或 `alreadyEnqueued && !priority` 返回 false → 没真正 enqueue
- channel 被 enqueue 过又被 `pollNext` 清掉，但 channel.getNextBuffer 返回了 `Optional.empty()` 或 `nextDataType=NONE` → gate 不 re-enqueue，再没人唤醒

逐条用 OQL/GUI 验证字段状态、用 log 验证事件顺序。**不要靠"我觉得应该"，要靠"heap 字段证明"。**

## 6. 排查中给用户的报告里要分清楚

- **heap 直接证明的事实**（"这个字段值是 X"）— 这是硬证据。
- **基于代码 reading 推断的事件序列**（"我猜先发生 A 后发生 B"）— 这是假设，要么用 log 验证，要么承认是假设。

不要把假设包装成结论。如果不确定，明说不确定。

## 7. 修复方案验证

提出修复方案后，最快的验证不是把整条因果链再推一遍，而是：**改一行加个唤醒/log，重跑，看 hang 是否消失**。1 分钟实验比 1 小时推理更靠谱。
