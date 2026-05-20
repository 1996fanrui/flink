# 验收方案：LocalInputChannel 解耦 toBeConsumedBuffers

> 本次为纯重构，**不新增任何测试**，仅依赖 FLINK-39018 及其准备阶段已有 9 个回归测试做行为保证。

## 状态表

| 编号 | 测试内容概要 | 需求ID列表 | 状态 | 测试执行方 | 备注 |
|------|------------|-----------|------|-----------|------|
| AT-EP3P | FLINK-39018 及其准备阶段已有 9 个与 `toBeConsumedBuffers`/recovery 相关的 LocalInputChannelTest 测试不修改测试代码继续通过 | REQ-7QPN, REQ-OGCD, REQ-1RUH, REQ-8WJ8, REQ-MJTH, REQ-J3CS | 待测试 | 代码自动化 | |

---

## 验收步骤

### [L1-测试] AT-EP3P FLINK-39018 及其准备阶段已有测试零修改通过

**目的**：本次解耦是纯重构，必须保证既有与 `toBeConsumedBuffers`/recovery 相关的回归测试一字不改即可通过。覆盖范围：FLINK-39018 引入的 7 个测试（`testCheckpointStartedPersistsRecoveredBuffers`、`testPriorityEventConsumedBeforeRecoveredBuffers`、`testPriorityEventFailsFastWhenSubpartitionViewIsNull`、`testPriorityEventFailsFastWhenNonPriorityBufferReturned`、`testPriorityEventFailsFastWhenSubpartitionViewReturnsNull`、`testMultipleConsecutivePriorityEvents`、`testNextDataTypeCorrectedToRecoveredBufferType`）+ 准备阶段引入的 2 个测试（`testGetBuffersInUseCountIncludesToBeConsumedBuffers`、`testGetNextBufferWithMigratedRecoveredBuffers`）。

**命令**：

```bash
./mvnw -pl flink-runtime test -Dtest='LocalInputChannelTest#testCheckpointStartedPersistsRecoveredBuffers+testPriorityEventConsumedBeforeRecoveredBuffers+testPriorityEventFailsFastWhenSubpartitionViewIsNull+testPriorityEventFailsFastWhenNonPriorityBufferReturned+testPriorityEventFailsFastWhenSubpartitionViewReturnsNull+testMultipleConsecutivePriorityEvents+testNextDataTypeCorrectedToRecoveredBufferType+testGetBuffersInUseCountIncludesToBeConsumedBuffers+testGetNextBufferWithMigratedRecoveredBuffers' -P java11-target -P java11
```

**预期结果**：Maven 命令退出码 0；surefire 报告 9 项 PASS、0 项 FAIL/ERROR。

---

## 备注

- 验收过程必须使用 `flink-test-runner` sub agent 执行上述命令
- 若 mvn 报 unresolved symbol，执行 `./mvnw clean install -U -Pfast -DskipTests -P java11-target -P java11` 重新编译后再运行测试
