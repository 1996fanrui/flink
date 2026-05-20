# Fix Commit 规范

当前分支已按 Phase 1–5 拆分为 5 个独立 commit。后续修复阶段产生的所有 commit 统一称为 **fix commit**，遵循以下规范，便于后续 squash / rebase 时按 phase 合并。

## 规则 1：一个 fix commit 只归属一个 phase

修复任何问题前，先判定该问题属于哪个 phase（Phase 1 / 2 / 3 / 4 / 5），然后为该 phase 单独创建一个 fix commit。一个 fix commit 不允许同时修改多个 phase 的代码。

## 规则 2：commit message 必须标注归属 phase

格式：

```
[FLINK-38544][fix][phaseN] <简述>
```

示例：

```
[FLINK-38544][fix][phase3] Correct SpillFile flush ordering
```

合并时按 `[phaseN]` 标记将 fix commit squash 进对应的 phase commit。

## 规则 3：按文件归属 phase

所有文件按其所属 phase 归类；唯一的例外是：

- `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoveredInputChannel.java`
  跨 Phase 1 / 2 / 4。修改此文件时按改动语义决定归属哪个 phase commit。

除上述文件外，所有其他文件的改动只允许归到其所在的唯一 phase。

## 规则 4：修复期不引入新测试

当前主流程尚未跑通，大量新增测试可能基于错误前提。修复期遵循：

- 保留现有测试，确保能跑
- 接口/签名变更时，仅修改对应的现有测试
- 不新增单元测试或集成测试
- 主流程跑通后再统一回补测试

## 操作 checklist（每次修复前）

1. 定位问题 → 判定 phase（1/2/3/4/5）
2. 仅改动该 phase 范围内的文件（`RecoveredInputChannel.java` 按改动语义归属）
3. 仅在接口变化时同步修改现有测试，不新增测试
4. commit message 使用 `[FLINK-38544][fix][phaseN] <简述>` 格式
