# 验收方案：Phase 1 — 公共接口与 sentinel 类型骨架

> Phase 1 只引入接口与类型骨架，不改变任何运行时行为，因此验收只覆盖"编译通过 + 既有测试零修改回归 + 静态结构断言"。本 phase 不新增测试用例。

## 状态表

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-PF3Z | flink-runtime 模块整体编译通过（接口、骨架、可见性提升全部纳入） | REQ-AYII, REQ-43Q8, REQ-KDF1, REQ-9FMG, REQ-9MCR, REQ-GYJN, REQ-KX4N | 通过 | 代码自动化 | `./mvnw -pl flink-runtime -am -DskipTests clean install`，BUILD SUCCESS（prior session） |
| AT-HREU | flink-runtime 现有 `o.a.f.runtime.io.network.partition.consumer.*` 与 `o.a.f.runtime.checkpoint.channel.*` 测试包零修改全部通过 | REQ-AYII, REQ-43Q8, REQ-KDF1, REQ-9FMG, REQ-9MCR, REQ-GYJN, REQ-KX4N | 通过 | 代码自动化 | 6 test classes all PASS（prior session） |
| AT-Y49R | Agent 静态确认 Phase 1 新增文件与可见性变更点真实落地（grep 比对 5 个新建文件 + `ChannelStateWriter` 的 default no-op + `RecoveredInputChannel.releaseAllResources` 已为 `public`） | REQ-AYII, REQ-43Q8, REQ-KDF1, REQ-9FMG, REQ-9MCR, REQ-GYJN, REQ-KX4N | 通过 | Agent 执行 | ls 确认 5 个文件存在；addInputDataFromSpill grep 2 处命中（声明+no-op）；releaseAllResources 第 320 行含 public 修饰符 |

---

## 验收步骤

### [L1-测试] AT-PF3Z flink-runtime 模块整体编译通过

**目的**：确保 Phase 1 引入的 6 个新文件、`ChannelStateWriter` 接口追加方法、`RecoveredInputChannel.releaseAllResources` 可见性提升后，整个 `flink-runtime` 模块（含所有 `ChannelStateWriter` 实现 / mock / fake）继续编译通过。

**命令**：

```bash
./mvnw -pl flink-runtime -am -DskipTests -P java11-target -P java11 -Pfast clean install
```

**预期结果**：Maven 命令退出码 0；无 `cannot find symbol` / `incompatible types` / 接口实现缺失等编译错误。

---

### [L1-测试] AT-HREU 现有 partition.consumer + checkpoint.channel 测试零修改通过

**目的**：Phase 1 不引入任何行为变化，因此 `o.a.f.runtime.io.network.partition.consumer` 与 `o.a.f.runtime.checkpoint.channel` 现有测试集合**一字不改**即应继续 PASS。

**命令**：

```bash
./mvnw -pl flink-runtime test -P java11-target -P java11 \
    -Dtest='LocalInputChannelTest,RemoteInputChannelTest,RecoveredInputChannelTest,ChannelStateWriterImplTest,ChannelStatePersisterTest,SequentialChannelStateReaderImplTest'
```

**预期结果**：Maven 命令退出码 0；surefire 报告对应 6 个测试类全部 PASS、无 FAIL/ERROR。

---

### [L2-Agent] AT-Y49R Agent 静态确认骨架文件与可见性变更点

**目的**：确认 Phase 1 设计要求落地（不依赖运行时行为，仅做静态结构断言）。

**采集命令**（Agent 执行；证据目录由 Agent 调用前导出 `EVIDENCE_DIR=$(mktemp -d /tmp/agent-tmp/review/at-y49r.XXXXXX)` 绑定，下文统一引用 `$EVIDENCE_DIR`）：

```bash
EVIDENCE_DIR=$(mktemp -d /tmp/agent-tmp/review/at-y49r.XXXXXX)

# 1. 5 个新建文件存在
ls flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/RecoveryCheckpointTrigger.java \
   flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoverableInputChannel.java \
   flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/BufferRequester.java \
   flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/RecoveryCheckpointBarrier.java \
   flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/DiskSnapshot.java \
   > "$EVIDENCE_DIR/files_exist.txt"

# 2. ChannelStateWriter.addInputDataFromSpill 已声明且接口处有 default no-op
grep -n "addInputDataFromSpill" \
   flink-runtime/src/main/java/org/apache/flink/runtime/checkpoint/channel/ChannelStateWriter.java \
   > "$EVIDENCE_DIR/addInputDataFromSpill_decl.txt"

# 3. RecoveredInputChannel.releaseAllResources 已为 public
grep -n "releaseAllResources" \
   flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoveredInputChannel.java \
   > "$EVIDENCE_DIR/releaseAllResources_visibility.txt"

echo "Evidence written to $EVIDENCE_DIR"
```

**判定命令**：人工/Agent 比对采集输出：

- `files_exist.txt` 内 `ls` 命令退出码 0、5 行（5 个文件全部命中）
- `addInputDataFromSpill_decl.txt` 命中至少 2 次（接口声明 + default no-op，或接口声明 + `NO_OP` 显式 override）
- `releaseAllResources_visibility.txt` 中匹配到的行包含 `public` 修饰符

**清理命令**：

```bash
rm -rf "$EVIDENCE_DIR"
```

**预期结果**：上述三组静态结构断言全部满足。

---

## 备注

- 验收 L1 步骤必须由 `flink-test-runner` sub agent 执行
- 若 mvn 报 unresolved symbol，先执行 `./mvnw clean install -U -Pfast -DskipTests -P java11-target -P java11` 重新编译再重试
- 本 phase 不引入 ITCase；ITCase 在 Phase 5 引入
