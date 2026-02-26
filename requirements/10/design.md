# 设计文档 - Flink测试自动化执行工具

## 1. 总体架构

### 1.1 Skill结构
```
.claude/skills/flink-test-analyzer/
├── SKILL.md                    # Coordinator agent (orchestrates 8 phases via Task tool)
├── scripts/
│   ├── run_tests.sh            # Test execution loop script
│   ├── parse_logs.py           # Log parsing script
│   ├── split_failure_logs.py   # Failure log splitting script
│   ├── generate_report.py      # Report generation script
│   └── find_failures.sh        # Quick failure location script
├── templates/
│   └── report.md               # Report format reference
└── tests/                      # Unit tests for Python scripts
```

### 1.2 架构模式

采用单一Coordinator + Task tool sub agent模式：

- **SKILL.md** 是唯一的协调agent文件，定义完整的8阶段工作流
- 协调agent不直接执行命令，通过Task tool将每个阶段委托给sub agent执行，每个阶段至少包含2个sub agent：执行agent + 独立验证agent
- 验证agent不通过时，重新运行执行agent，直到验证通过后再进入下一步
- 每个sub agent收到自包含的指令（包含所有必要上下文），执行具体的Shell/Python命令

#### 8个执行阶段
1. **环境准备**：提交本地代码、创建worktree
2. **测试发现**（条件执行）：口语化描述转换为测试类列表
3. **项目编译**：调用build_with_specific_version.sh
4. **预执行期望生成**：分析测试源码估算测试用例数量，建立验证基准
5. **测试执行**：循环执行测试、处理checkstyle错误
6. **结果分析**：调用Python脚本解析日志、生成报告
7. **归档**：将日志和报告归档到主仓库 `log/` 目录
8. **清理**：删除worktree和临时分支

## 2. 数据流设计

### 2.1 配置数据
包含测试列表、循环次数（默认100）、单次超时时间、worktree路径和日志目录等关键信息。

### 2.2 执行状态
维护当前循环次数、总执行次数、成功/失败统计、checkstyle错误列表和细粒度测试结果。

## 3. 脚本设计

### 3.1 run_tests.sh
- 基于loop.sh改造，支持多测试执行
- 参数化：测试列表、循环次数、超时时间
- 输出结构化日志

### 3.2 parse_logs.py
- 解析Flink测试日志格式
- 识别测试开始/结束标记
- 提取参数化测试信息
- 输出JSON格式结果

### 3.3 generate_report.py
- 读取解析结果
- 计算双粒度统计
- 生成双文件报告（摘要report.md + 失败详情failure_details.md）
- 摘要报告只包含统计表格（细粒度只列失败项），失败详情单独成文件

### 3.4 split_failure_logs.py
- 从完整日志中拆分失败test case的日志
- 拆分范围：从测试开始标记到测试结束标记之间的完整日志
- 文件命名：`{TestClass}_{method}[{params}]_from_{原始日志文件名}.log`
- 输出到独立子目录 `split_failures/`

### 3.5 find_failures.sh
- 封装用户提供的awk命令快速定位失败（基于 `[ERROR] Errors:` 和 `Tests run:` 标记匹配）
- 支持批量日志文件处理
- 输出格式化失败列表

## 4. 错误处理原则

- **Checkstyle错误**：检测到后尝试自动修复，无法修复则终止并报告
- **编译错误**：尝试clean后重编译，失败则终止流程
- **测试超时**：记录为超时失败，强制终止后继续下一次循环

## 5. Worktree管理

### 5.1 创建策略
- 路径格式：`/tmp/claude-tmp/flink-test-{YYYYMMDD_HHmmss}`
- 基于当前分支创建
- 独立的日志目录

### 5.2 归档策略
- 归档目录：`{主仓库}/log/flink-test-analyzer_{YYYYMMDD_HHmmss}/`
- 归档内容：
  - 所有包含失败的原始日志文件
  - `split_failures/` 拆分后的失败日志子目录
  - 摘要报告（report.md）和失败详情报告（failure_details.md）
- 归档在worktree清理之前执行

### 5.3 清理策略
- 归档完成后清理worktree
- 异常退出时注册cleanup handler
- 定期清理超过24小时的旧worktree

## 6. Checklist设计

> 每个Checklist由独立的验证agent执行，与执行agent分离，确保验证的客观性。

### 6.1 环境准备Checklist
- [ ] 本地代码已提交
- [ ] worktree创建成功
- [ ] 切换到worktree目录

### 6.2 编译Checklist
- [ ] Maven命令执行成功
- [ ] target目录存在
- [ ] 无编译错误

### 6.3 测试执行Checklist
- [ ] 测试命令执行
- [ ] 日志文件生成
- [ ] 无checkstyle错误或已修复

### 6.4 结果分析Checklist
- [ ] 所有日志文件已解析
- [ ] 失败日志已拆分到独立文件
- [ ] 统计数据完整
- [ ] 报告生成成功

### 6.5 归档Checklist
- [ ] 归档目录已创建
- [ ] 失败原始日志已复制
- [ ] 拆分日志已复制
- [ ] 报告已保存
- [ ] commit信息已记录

### 6.6 清理Checklist
- [ ] worktree已删除
- [ ] 临时文件已清理

## 7. 报告格式设计

### 7.1 概要部分
- 执行时间和环境信息
- 基于的commit hash和branch名称
- 测试范围（类/方法列表）
- 用户原始需求记录
- 总体成功率

### 7.2 粗粒度统计
- 总循环次数
- 完全成功次数
- 部分失败次数
- 完全失败次数

### 7.3 细粒度统计
- 按test case分组
- 参数化测试展开
- 每个组合的成功/失败次数
- 失败率排序
- 仅列出失败的test case，成功的不逐条列出（在总数中体现）

### 7.4 失败详情
- 独立文件（failure_details.md），包含完整stack trace
- 对应的拆分日志文件路径（可直接打开查看）

### 7.5 追溯信息
- 报告包含commit hash、branch名称、用户测试请求等追溯信息（不生成单独文件）

