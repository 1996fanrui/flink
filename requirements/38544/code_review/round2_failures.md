# Round 2 — `rui_tools/loop.sh` test failures

Log: `log/20260522_170556.log`

## 总览

| Test class | Run | Pass | Fail |
|---|---|---|---|
| `UnalignedCheckpointITCase` | 11 | 11 | 0 |
| `UnalignedCheckpointRescaleWithMixedExchangesITCase` | 5 | 5 | 0 |
| **`UnalignedCheckpointRescaleITCase`** | **50** | **43** | **7** |

7 failed parameterizations，全部来自 `UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint`：

| TestInfo idx | 参数 |
|---|---|
| 7 | downscale keyed_broadcast from 7 to 2, sourceSleepMs = 0 |
| 9 | downscale keyed_broadcast from 5 to 3, sourceSleepMs = 5 |
| 15 | upscale pipeline from 1 to 2, sourceSleepMs = 0 |
| 19 | upscale pipeline from 20 to 21, sourceSleepMs = 0 |
| 23 | downscale pipeline from 7 to 3, sourceSleepMs = 0 |
| 26 | downscale pipeline from 5 to 3, sourceSleepMs = 5 |
| 34 | upscale union from 3 to 5, sourceSleepMs = 5 |

## 根因 — **同一个 bug**，7 次失败堆栈一致

任务（`rebalance0`、`failing-map`、`upscale0` 等）刚 INITIALIZING → RUNNING 就被切到 FAILED，failure cause：

```
java.util.concurrent.RejectedExecutionException:
  Task ... rejected from java.util.concurrent.ThreadPoolExecutor@xxxx
    [Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 1]
  at StreamTask.submitDrainIfFilterOn(StreamTask.java:1007)
  at StreamTask.lambda$restoreStateAndGates$9(StreamTask.java:964)
  at StreamTaskActionExecutor$1.runThrowing(StreamTaskActionExecutor.java:50)
  at Mail.run / MailboxProcessor.runMail / processMailsNonBlocking
```

执行器是 `channelIOExecutor`，状态是 **Terminated**。

## 时序（在 `StreamTask` 内）

```
restoreStateAndGates(845)
  ├── 注册 thenRun(suspend) on allRecoveredFuture = allOf(recoveredFutures)
  │     recoveredFutures = bufferFilteringCompleteFutures 每个 gate 一个
  └── 注册 thenRun(enqueue submitDrain mail) on allConverted
        allConverted = allOf(conversionDoneFutures)
        conversionDoneFutures.complete() 由 requestPartitions mail 处理后置位

runMailboxLoop(848)   ← 第一次：restore 期间
  - 处理 requestPartitions × N mail（每个完成 conversionDone[i]）
  - 处理 suspend mail：mailbox 退出循环   ← 抢在 submitDrain 之前
  - submitDrain mail 还留在队列里 ← (1)

checkState(allGatesRecoveredFuture.isDone)(857)        ← 通过（因为 recoveredFutures 都已完成）
channelIOExecutor.shutdown()(862)                       ← 关掉 executor  ← (2)
... 任务转入 RUNNING ...
mailboxProcessor.runMailboxLoop()（main loop, 1056）  ← 第二次：进入主循环
  - 终于处理 (1) 中遗留的 submitDrain mail
  - submitDrain 调用 `channelIOExecutor.execute(spillReader::drain)`
  - 但 executor 已 (2) 关闭并 Terminated → RejectedExecutionException
  - 任务 FAILED → graph 全局 FAILED → 测试 assertion 失败 "Graph is in globally terminal state (FAILED)"
```

`recoveredFutures`（drives suspend）和 `conversionDoneFutures`（drives drain submit）是**异步的两条链**：suspend 的链子比 submit-drain 的链子短一节（少了 mailbox 内的 `requestPartitions` 一拍），所以经常会 suspend 先到。

## 修复方案 — 单次 submit + I/O 线程 wait

### 结构性根因

bug 的本质是 **drain 是一次独立 submit**：

- 第一次 submit（line 908）：filter runnable，跑完即结束 → I/O 线程回到 idle
- 第二次 submit（line 1007，由 submitDrainIfFilterOn 触发）：drain runnable，**晚于** line 862 的 shutdown 才到达

两次 submit 之间存在不可消除的"裂缝"：drain 需要"physical channel"产物，而这个产物只能由 task 线程的 `requestPartitions` 生成；task 线程要先跑完 conversion mail 才能转手 submit drain，而 suspend mail 经常抢先一拍让 mailbox 退出 → 一旦 mailbox 退出，line 862 的 shutdown 就跑了，drain submit 必然被拒。

只关 executor 时机（早关、延关、条件关）治标不治本：只要"drain 是一次独立 submit"这一结构不变，竞争窗口就一直存在。

### 方案 — 把 filter + drain 合并成 channelIOExecutor 的**一次** submit

`channelIOExecutor.execute(...)` 提交 ONE runnable，runnable 内部连续做：
1. `reader.readInputData(...)` — filter，跑完不返回
2. `SpillFileReader reader = drainHandoff.get()` — **block 在这里**，等 task 线程把 SpillFileReader 通过 `CompletableFuture` 交付过来
3. 拿到 reader 后立刻 `reader.drain()`
4. finally `reader.close()`
5. runnable 返回 → I/O 线程退出 → executor 自然 Terminated

task 线程侧（mail）做的事不变：
1. 跑 `requestPartitions` 完成 conversion
2. 所有 gate conversion 完成后，在 mail 中：collect physical channels、build `SpillFileReader`、stash `recoveryCheckpointTrigger`、**`drainHandoff.complete(reader)`** 把 reader 交付给 I/O 线程

`drainHandoff.complete(null)` 表示"filter-off / 无 spill 产物"，I/O 线程拿到 null 直接退出，不 drain。

### 为什么这样可以走通

- `ExecutorService.shutdown()` **不打断正在运行的任务**，只是禁止新任务入列。line 862 的 shutdown 在 I/O runnable（filter + wait + drain）还在跑的时候被调用是合规状态——它只是不再接受新 submit，runnable 会跑完才让 executor 真正 Terminated。
- 没有第二次 submit，自然没有"submit 时 executor 已 Terminated"。
- 没有死锁：conversion 是 task 线程自己的活，不依赖 I/O 线程，所以 I/O 线程 wait 不会卡住 task 线程的进展。
- master 路径完全不变：`checkpointingDuringRecoveryEnabled == false` 时，runnable 跑完 filter 就 return（与 master 一致），不进 wait 阶段。

### 异常路径必须保证 drainHandoff 一定被 complete

- filter 抛异常 → catch 里 `drainHandoff.completeExceptionally(e)`，I/O 线程不会无限挂死
- conversion 抛异常（line 941 已有 `conversionDone.completeExceptionally`）→ `allConverted` 失败 → `whenComplete` 接到 err → `drainHandoff.completeExceptionally(err)`
- build SpillFileReader 抛异常 → catch 里 `drainHandoff.completeExceptionally`

I/O 线程 `.get()` 抛 `ExecutionException` 时，调 `asyncExceptionHandler.handleAsyncException` 上抛（沿用现有 pattern）。

### 具体改动（`StreamTask.restoreStateAndGates` 内）

文件归属：`StreamTask.java` 属于 **Phase 4**。commit 为 `[FLINK-38544][fix][phase4]`。

涉及的编辑：

1. 在 `checkpointingDuringRecoveryEnabled == true` 时声明 `CompletableFuture<SpillFileReader> drainHandoff = new CompletableFuture<>();`（filter-off 路径不需要）
2. 修改 line 908 的 `channelIOExecutor.execute(...)` 内容：
   - filter 后增加 `if (drainHandoff != null) { 等待 → drain → close }` 块
   - filter catch 块增加 `if (drainHandoff != null) drainHandoff.completeExceptionally(e)`
3. 删除 `submitDrainIfFilterOn` 方法的 **second submit**：原方法的"建 SpillFileReader + stash trigger"逻辑保留，但末尾的 `channelIOExecutor.execute(spillReader::drain)` 改成 `drainHandoff.complete(spillReader)`
4. 把 `allConverted.thenRun(... mainMailboxExecutor.execute(submitDrainIfFilterOn) ...)` 的入口保持（仍在 task 线程上构建 reader），mail 体内末尾改为 complete handoff future
5. `allConverted` 异常路径（whenComplete 的 err 分支）→ `drainHandoff.completeExceptionally(err)`

line 862 的 `channelIOExecutor.shutdown()` 不动——保留它能让 master 路径行为不变，且对新路径无害（不打断 running task）。

### 验证

实施后跑 `./mvnw -T 20 clean install -U -Pfast -DskipTests -P java11-target -P java11` + `bash rui_tools/loop.sh` 一轮，三个 ITCase 全 0 失败方可 squash 进 Phase 4。
