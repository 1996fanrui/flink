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

## 6. 排查中给用户的报告里要分清楚

- **heap 直接证明的事实**（"这个字段值是 X"）— 这是硬证据。
- **基于代码 reading 推断的事件序列**（"我猜先发生 A 后发生 B"）— 这是假设，要么用 log 验证，要么承认是假设。

不要把假设包装成结论。如果不确定，明说不确定。

## 7. 修复方案验证

提出修复方案后，最快的验证不是把整条因果链再推一遍，而是：**改一行加个唤醒/log，重跑，看 hang 是否消失**。1 分钟实验比 1 小时推理更靠谱。
