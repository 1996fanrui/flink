# 验收测试方案

## 验收状态表

| 编号 | 测试内容概要 | 状态 | 测试执行方 | 备注 |
|------|------------|------|-----------|------|
| T1.1 | 相同测试去重 | 通过 | 代码自动化 | `python -m pytest tests/test_deduplicate_failures.py -v -k "test_same_test"` 1 passed |
| T1.2 | 相同根因去重 | 通过 | 代码自动化 | `python -m pytest tests/test_deduplicate_failures.py -v -k "root_cause"` 1 passed |
| T1.3 | 高频优先分析策略 | 通过 | 代码自动化 | `python -m pytest tests/test_deduplicate_failures.py -v -k "high_frequency or similarity"` 2 passed |
| T1.4 | 无失败时跳过分析 | 通过 | 代码自动化 | `python -m pytest tests/test_deduplicate_failures.py -v -k "no_failures or empty"` 2 passed |
| T1.5 | Phase 6.5 集成到 SKILL.md | 通过 | Agent 执行 | 读取 SKILL.md 确认: Phase 6.5 位于 Phase 6(L175) 和 Phase 7(L260) 之间(L216)，含 Checklist+Verification，Phase 7 归档含 root_cause_report.md，Final Output 含提示 |
| T1.6 | 归档包含新报告 | 通过 | Agent 执行 | 读取 SKILL.md Phase 7 确认: report.md(L277)、failure_details.md(L280)、root_cause_report.md(L293) 均在归档逻辑中 |
| T2.1 | CLAUDE.md 原则完整性 | 通过 | Agent 执行 | 读取 CLAUDE.md 确认: 含"Skill 开发原则"章节(L15)，覆盖 Skill 结构、分层架构、确定性操作、验证机制、资源管理、错误处理六个类别 |
| T2.2 | 原则可操作性验证 | 待测试 | 人工 | |

## 任务 1: 根因分析功能验收

### T1.1 [L1-测试] 相同测试去重

**测试目标**：验证同一测试（类+方法+参数）多次失败只保留一个代表

**测试步骤**：
1. 准备 parse_results.json，包含同一测试 `TestA.method1[param1]` 在 5 个迭代中都失败
2. 执行去重脚本
3. 验证输出中 `TestA.method1[param1]` 只出现在一个失败组中

**预期结果**：
- 去重后该测试只出现一次，不重复分析

### T1.2 [L1-测试] 相同根因去重

**测试步骤**：
1. 准备 parse_results.json，包含：
   - `TestA.method1` 抛出 `NullPointerException at Foo.java:42`
   - `TestB.method2` 抛出 `NullPointerException at Foo.java:42`
   - `TestC.method3` 抛出 `TimeoutException at Bar.java:100`
2. 执行去重脚本
3. 验证输出分为 2 组（NPE 一组、Timeout 一组）

**预期结果**：
- 相同异常类型+相同栈追踪的失败归为一组
- 不同异常类型的失败分为不同组

### T1.3 [L1-测试] 高频优先分析策略

**测试步骤**：
1. 准备 parse_results.json，包含：
   - 500 个测试失败，异常指纹相同（高频组）
   - 3 个测试失败，异常指纹与高频组相似但不确定是否相同
   - 2 个测试失败，异常指纹明确不同（独立组）
2. 执行去重脚本
3. 验证输出：
   - 高频组排在第一位
   - 输出结果按频次降序排列
   - 高频组标记为"需分析"
   - 独立组标记为"需分析"
   - 不确定的组标记为"可能与高频组相似"

**预期结果**：
- 高频组优先
- 确定不同根因的独立分析
- 不确定的不强制分析

### T1.4 [L1-测试] 无失败时跳过分析

**测试步骤**：
1. 准备 parse_results.json，所有测试通过，无失败
2. 执行去重脚本
3. 验证输出为空的失败组

**预期结果**：
- 输出空组，不触发任何分析

### T1.5 [L2-Agent] Phase 6.5 集成到 SKILL.md

**测试目标**：验证 SKILL.md 正确包含新阶段

**测试步骤**：
1. Agent 读取更新后的 SKILL.md
2. 验证包含 Phase 6.5: Root Cause Analysis
3. 验证 Phase 6.5 位于 Phase 6 之后、Phase 7 之前
4. 验证 Phase 6.5 包含 Checklist 和 Verification 步骤
5. 验证 Phase 7 的归档逻辑包含 root_cause_report.md
6. 验证 Final Output 包含 root_cause_report.md 的提示

**预期结果**：
- 新阶段正确嵌入现有流程
- 与现有阶段的验证模式一致

### T1.6 [L2-Agent] 归档包含新报告

**测试目标**：验证归档目录包含所有三个报告

**测试步骤**：
1. Agent 检查 Phase 7 的归档逻辑
2. 确认归档目标包含：report.md、failure_details.md、root_cause_report.md

**预期结果**：
- 归档目录包含全部三个报告

## 任务 2: Skill 开发原则验收

### T2.1 [L2-Agent] CLAUDE.md 原则完整性验证

**测试步骤**：
1. Agent 读取更新后的 CLAUDE.md
2. 验证包含"Skill 开发原则"章节
3. 验证涵盖以下原则类别：
   - 架构模式（Coordinator-Worker、验证分离、阶段化）
   - 任务分配策略（确定性任务用脚本、判断性任务用 LLM）
   - 验证机制（Checklist、独立验证、失败重试）
   - 资源管理（隔离执行、强制清理、归档）
   - 错误处理（分级恢复、关键路径保护）

**预期结果**：
- 所有原则类别存在且内容具体可操作

### T2.2 [L3-人工] 原则可操作性验证

**测试步骤**：
1. 人工选择一个简单的新 skill 需求
2. 按照 CLAUDE.md 中的原则设计 skill 架构
3. 评估原则是否覆盖主要决策点、提供清晰选择标准

**预期结果**：
- 原则能指导新 skill 的设计和开发