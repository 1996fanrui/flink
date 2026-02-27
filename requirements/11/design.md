# 设计文档

## 概述

本设计包含两个独立任务：
1. 增强 flink-test-analyzer skill，添加根因分析功能
2. 提取 skill 开发最佳实践到项目 CLAUDE.md

## 任务 1: 根因分析增强设计

### 1.1 架构设计

#### 1.1.1 新增组件

**Python 脚本**：
- `scripts/deduplicate_failures.py`：失败去重和分组（确定性操作，脚本实现）

**SKILL.md 新增阶段**：
- Phase 6.5: Root Cause Analysis（在现有 Phase 6 和 Phase 7 之间）
- 该阶段调用 sub agent 完成根因分析

#### 1.1.2 流程位置

在 SKILL.md 的 Phase 6（Result Analysis）之后，Phase 7（Archive）之前，插入新的 Phase 6.5：Root Cause Analysis。

### 1.2 去重策略设计

#### 1.2.1 两级去重

**第一级：相同测试去重**（脚本实现，确定性操作）
- 基于测试类、方法名、参数组合唯一标识一个测试
- 同一测试在多次迭代中失败，只保留一个代表性失败

**第二级：相同根因去重**（脚本实现，确定性操作）
- 基于错误特征分组：异常类型、错误消息（标准化后去除时间戳等变量部分）、栈追踪关键部分
- 特征相同的失败归为同一组，只分析一次

#### 1.2.2 分析策略原则

**高频优先、不确定时保守分析**：
- 去重分组后，按频次降序排列
- 优先分析最高频的失败组
- 不确定是否属于同一根因时，宁可少分析，不过度拆分
- 能 100% 确定是不同根因的，分别分析
- 不确定的低频组标注"可能与某高频组相似"，不强制分析
- 采用迭代式修复思路：修复最高频问题后再运行测试，剩余问题下一轮处理

### 1.3 分析流程设计

#### 1.3.1 Phase 6.5: Root Cause Analysis

流程步骤：
1. **去重**：执行去重脚本，输入 parse_results.json，输出去重后的失败分组
2. **分析**：对每个需要分析的失败组，调用 sub agent 进行根因分析
3. **报告生成**：由 sub agent 汇总分析结果，生成 root_cause_report.md

跳过条件：如果没有失败的测试，跳过整个 Phase 6.5。

#### 1.3.2 Sub Agent 职责

负责根因分析的 sub agent：
- 接收去重后的失败组信息（包含错误消息、栈追踪、影响的测试列表）
- 分析错误模式和根本原因
- 提供修复建议
- 评估影响范围
- 对不确定的低频组标注相似性

#### 1.3.3 Checklist 和 Verification

与现有 Phase 保持一致的验证模式：
- Checklist: 去重结果生成、分析完成、报告生成
- Verification: 独立验证 agent 检查产出物是否完整

### 1.4 报告设计

**root_cause_report.md** 基本结构：
- Summary：总根因数、影响测试数、分析时间
- Root Cause Groups：每个根因组包含——影响测试列表、错误模式描述、根因分析、修复建议
- 未分析组（如有）：不确定的低频组，标注与哪个高频组可能相似

### 1.5 归档集成

Phase 7（Archive）需要额外归档：
- root_cause_report.md
- deduplicated_failures.json

Phase 8（Final Output）需要额外输出：
- 提示用户查看 root_cause_report.md

## 任务 2: Skill 开发原则提取

### 2.1 需要提取的原则

从 flink-test-analyzer 的实现中提取以下最佳实践类别：

1. **分层架构原则**：Coordinator agent 模式、子任务委托、验证 agent 分离
2. **确定性操作原则**：确定性任务用脚本（快、准、省 token），LLM 仅用于需要理解和判断的任务
3. **验证机制原则**：每个执行阶段配套验证阶段、Checklist 驱动、验证失败自动重试
4. **资源管理原则**：Worktree 隔离、清理保证（即使失败也要清理）、归档策略
5. **错误处理原则**：分级恢复策略、关键失败点识别

### 2.2 实施位置

- 添加到 `/Users/ruifan/code/github/flink-os-2/CLAUDE.md` 文件
- 位置：在现有"开发原则"之后，"代码提交和合并规范"之前
- 新增章节标题："Skill 开发原则"
- 每个原则类别作为子章节，包含具体的指导规则
- 具体文档内容在开发阶段确定

## 风险与限制

1. **指纹准确性**：错误指纹算法可能需要迭代优化，初版采用保守策略
2. **分析质量**：Agent 分析质量依赖于错误信息的完整性

## 实施计划

### Phase 1: 脚本开发
1. 实现 deduplicate_failures.py（含单元测试）

### Phase 2: Skill 集成
1. 修改 SKILL.md 添加 Phase 6.5
2. 更新 Phase 7 和 Final Output 的归档逻辑

### Phase 3: 文档更新
1. 更新 CLAUDE.md 添加 skill 开发原则