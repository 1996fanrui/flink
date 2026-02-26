# 验收测试方案

## 测试状态表

| 编号 | 测试内容概要 | 状态 | 测试执行方 | 备注 |
|------|------------|------|-----------|------|
| TC01 | 环境准备功能 | 通过 | Agent 执行 | 检查SKILL.md Phase 1: auto-commit逻辑、worktree路径`/tmp/claude-tmp/flink-test-{TIMESTAMP}`、目录验证步骤均存在; run_tests.sh第95行`mkdir -p "$LOG_DIR"`创建日志目录 |
| TC02 | 测试发现功能 | 通过 | Agent 执行 | 检查SKILL.md Phase 2: 搜索逻辑(grep/find)、用户确认步骤、显式类名时跳过Phase 2均存在; Input section定义了类名检测规则 |
| TC03 | 项目编译功能 | 通过 | Agent 执行 | 检查SKILL.md Phase 3: 调用`rui_tools/build_with_specific_version.sh`、失败重试一次后停止工作流均存在 |
| TC04 | 单测试执行 | 通过 | Agent 执行 | 检查run_tests.sh: `-t`参数接受测试类(第29行)、日志命名`${LOG_DIR}/${timestamp}.log`包含时间戳(第111行) |
| TC05 | 多测试执行 | 通过 | Agent 执行 | 检查run_tests.sh: `-t`参数直接传递给Maven `-Dtest`支持逗号分隔(第120行)、`-n`控制迭代次数(第30行、第109行循环) |
| TC06 | Checkstyle错误处理 | 通过 | Agent 执行 | 检查SKILL.md Phase 4第5步: checkstyle检测、auto-fix尝试、修复失败时停止工作流均存在 |
| TC07 | 日志解析功能 | 通过 | 代码自动化 | 执行`cd .claude/skills/flink-test-analyzer && uv run pytest tests/test_parse_logs.py -v`, 15个测试全部通过, 覆盖成功/失败/混合/参数化/空日志等场景 |
| TC08 | 粗粒度统计 | 通过 | 代码自动化 | 执行`uv run pytest tests/test_generate_report.py::TestCalculateCoarseStats -v`, 4个测试全部通过, 覆盖全通过/混合/全失败/空输入场景 |
| TC09 | 细粒度统计 | 通过 | 代码自动化 | pytest 26/26通过，含test_fine_stats_only_shows_failed_items和test_fine_stats_summary_totals |
| TC10 | 失败日志拆分 | 通过 | 代码自动化 | 执行`uv run pytest tests/test_split_failure_logs.py -v`, 12个测试全部通过, 覆盖单/多失败拆分、无失败返回空、文件命名含test名+参数+来源、成功test不拆分等场景 |
| TC11 | 日志归档到主仓库 | 通过 | Agent 执行 | E2E验收：归档目录`flink-test-analyzer_20260226_221602`，含report.md+failure_details.md+split_failures/+失败日志，无commit_info.txt/test_request.txt |
| TC12 | 报告生成（含commit和需求） | 通过 | Agent 执行 | E2E验收：report.md含commit/branch/test-request且无stack trace；failure_details.md含6个失败条目的完整stack trace |
| TC13 | 资源清理 | 通过 | Agent 执行 | 检查SKILL.md Phase 7: `git worktree remove --force`删除worktree、`git branch -D`清理临时分支; Error Handling section明确要求失败时也必须执行cleanup |
| TC14 | 异常处理 | 通过 | Agent 执行 | 检查run_tests.sh: SIGINT/SIGTERM trap(第92行)处理中断; 超时处理(第136-147行)30秒轮询后kill; SKILL.md Error Handling保证cleanup always runs |
| TC15 | 阶段独立验证机制 | 通过 | Agent 执行 | 代码审查：SKILL.md每个Phase后均有独立Verification block，含具体检查项和失败重试逻辑 |
| TC16 | 预执行期望生成 | 通过 | Agent 执行 | 代码审查：SKILL.md Phase 4定义预执行期望生成，通过grep @Test和检测参数化注解估算测试数量 |
| TC17 | 报告拆分为摘要+详情 | 通过 | 代码自动化 | pytest test_output_dir_writes_files通过；E2E验收确认report.md+failure_details.md双文件生成 |
| TC18 | 细粒度表格只列失败项 | 通过 | 代码自动化 | pytest test_fine_stats_only_shows_failed_items通过；E2E验收：66个test case中仅2个失败项出现在表格 |
| TC19 | commit和测试请求信息在报告中 | 通过 | 代码自动化 | pytest test_includes_test_request通过；E2E验收：report.md含commit/branch/test-request，归档无单独文件 |
| TC20 | 时间戳格式统一 | 通过 | Agent 执行 | E2E验收：时间戳20260226_221602含下划线；代码审查：SKILL.md和run_tests.sh格式一致 |
| TC21 | 归档目录带前缀 | 通过 | Agent 执行 | E2E验收：归档目录名`flink-test-analyzer_20260226_221602`；SKILL.md ARCHIVE_DIR定义正确 |

## 详细测试用例

### TC01: 环境准备功能 [L2-Agent]
**目标**：验证能正确准备测试环境

**前置条件**：
- 本地有未提交的代码修改

**测试步骤**：
1. 执行skill命令启动测试工具
2. 检查本地代码是否已提交
3. 验证worktree创建位置和命名

**预期结果**：
- 本地代码自动提交
- 在 `/tmp/claude-tmp/flink-test-{timestamp}` 创建worktree
- 成功切换到worktree目录

### TC02: 测试发现功能 [L2-Agent]
**目标**：验证口语化描述转换为具体测试

**测试步骤**：
1. 输入口语化测试描述："运行所有checkpoint相关的测试"
2. 等待agent调研
3. 确认返回的测试列表

**预期结果**：
- 返回包含UnalignedCheckpointITCase等相关测试类
- 用户可选择确认或修改测试列表

### TC03: 项目编译功能 [L2-Agent]
**目标**：验证项目编译流程

**测试步骤**：
1. 在worktree中执行编译
2. 检查编译输出
3. 验证target目录生成

**预期结果**：
- 调用build_with_specific_version.sh
- 编译成功无错误
- 生成必要的编译产物

### TC04: 单测试执行 [L2-Agent]
**目标**：验证单个测试的循环执行

**测试步骤**：
1. 指定单个测试类：`UnalignedCheckpointRescaleITCase`
2. 设置循环次数为5
3. 执行测试
4. 检查日志输出

**预期结果**：
- 测试循环执行5次
- 每次执行生成独立日志文件
- 日志包含完整的测试输出

### TC05: 多测试执行 [L2-Agent]
**目标**：验证多个测试同时执行

**测试步骤**：
1. 指定多个测试类
2. 设置循环次数为3
3. 执行测试
4. 验证所有测试都被执行

**预期结果**：
- 所有指定测试都被执行
- 每个测试都循环3次
- 日志正确分离

### TC06: Checkstyle错误处理 [L2-Agent]
**目标**：验证checkstyle错误的自动处理

**测试步骤**：
1. 故意引入checkstyle错误
2. 执行测试
3. 观察自动修复过程

**预期结果**：
- 检测到checkstyle错误
- 尝试自动修复
- 修复成功后继续执行
- 无法修复时停止并报告

### TC07: 日志解析功能 [L1-测试]
**目标**：验证日志解析脚本正确性

**测试代码**：
```python
def test_parse_test_logs():
    """测试日志解析功能"""
    sample_log = """
    ================================================================================
    Test execute[pipeline with local channels, p = 1, timeout = 0](org.apache.flink.test.checkpointing.UnalignedCheckpointITCase) is running.
    --------------------------------------------------------------------------------
    ...test output...
    --------------------------------------------------------------------------------
    Test execute[pipeline with local channels, p = 1, timeout = 0](org.apache.flink.test.checkpointing.UnalignedCheckpointITCase) successfully run.
    ================================================================================
    """

    results = parse_logs(sample_log)
    assert len(results) == 1
    assert results[0]['status'] == 'success'
    assert results[0]['test_name'] == 'UnalignedCheckpointITCase'
    assert results[0]['parameters'] == 'pipeline with local channels, p = 1, timeout = 0'
```

**预期结果**：
- 正确识别测试开始和结束标记
- 提取测试名称和参数
- 判断成功/失败状态

### TC08: 粗粒度统计 [L1-测试]
**目标**：验证循环级别的统计功能

**测试代码**：
```python
def test_coarse_statistics():
    """测试粗粒度统计"""
    test_results = {
        'iteration_1': {'total': 10, 'passed': 10, 'failed': 0},
        'iteration_2': {'total': 10, 'passed': 8, 'failed': 2},
        'iteration_3': {'total': 10, 'passed': 10, 'failed': 0},
    }

    stats = calculate_coarse_stats(test_results)
    assert stats['total_iterations'] == 3
    assert stats['fully_successful'] == 2
    assert stats['partially_failed'] == 1
    assert stats['success_rate'] == 2/3
```

**预期结果**：
- 正确统计总循环次数
- 识别完全成功的循环
- 计算整体成功率

### TC09: 细粒度统计 [L1-测试]
**目标**：验证test case级别的统计

**测试代码**：
```python
def test_fine_statistics():
    """测试细粒度统计"""
    test_data = [
        {'test': 'TestA', 'param': 'p1', 'status': 'success'},
        {'test': 'TestA', 'param': 'p1', 'status': 'success'},
        {'test': 'TestA', 'param': 'p1', 'status': 'failed'},
        {'test': 'TestA', 'param': 'p2', 'status': 'success'},
    ]

    stats = calculate_fine_stats(test_data)
    assert stats['TestA']['p1']['total'] == 3
    assert stats['TestA']['p1']['passed'] == 2
    assert stats['TestA']['p1']['failed'] == 1
    assert stats['TestA']['p2']['total'] == 1
```

**预期结果**：
- 按test case + 参数分组统计
- 正确计算每个组合的成功/失败次数
- 生成失败率排序

### TC10: 失败日志拆分 [L1-测试]
**目标**：验证失败日志正确拆分为独立文件

**测试代码**：
```python
def test_split_failure_logs():
    """测试失败日志拆分功能"""
    sample_log = """
    ================================================================================
    Test execute[p=1](org.apache.flink.TestA) is running.
    --------------------------------------------------------------------------------
    ...详细日志内容...
    --------------------------------------------------------------------------------
    Test execute[p=1](org.apache.flink.TestA) failed with:
    java.lang.AssertionError: expected:<1> but was:<2>
    ================================================================================
    ================================================================================
    Test execute[p=2](org.apache.flink.TestA) is running.
    --------------------------------------------------------------------------------
    ...成功的日志...
    --------------------------------------------------------------------------------
    Test execute[p=2](org.apache.flink.TestA) successfully run.
    ================================================================================
    """

    split_files = split_failure_logs(sample_log, "20260226_111126.log", output_dir)
    assert len(split_files) == 1  # 只有1个失败
    assert "TestA_execute[p=1]_from_20260226_111126" in split_files[0]
    # 文件内容包含从 is running 到 failed with 的完整日志
    content = open(split_files[0]).read()
    assert "is running" in content
    assert "failed with" in content
    assert "详细日志内容" in content
```

**预期结果**：
- 仅拆分失败的test case日志
- 拆分范围从开始标记到结束标记
- 文件命名包含test case名、参数和来源日志名
- 成功的test case不被拆分

### TC11: 日志归档到主仓库 [L2-Agent]
**目标**：验证日志和报告正确归档到主仓库

**测试步骤**：
1. 完成一轮测试执行和分析
2. 检查主仓库 `log/` 目录
3. 验证归档目录结构和内容

**预期结果**：
- `log/flink-test-analyzer_{TIMESTAMP}/` 归档目录已创建
- 包含失败的原始日志文件
- 包含 `split_failures/` 子目录及拆分后的日志
- 包含 report.md（测试摘要）
- 包含 failure_details.md（失败详情）
- 不包含 commit_info.txt 和 test_request.txt（这些信息已内嵌在报告中）

### TC12: 报告生成（含commit和需求） [L2-Agent]
**目标**：验证报告包含完整的追溯信息

**测试步骤**：
1. 完成一轮测试执行
2. 触发报告生成
3. 检查报告内容

**预期结果**：
- report.md包含执行概要、粗粒度和细粒度统计
- report.md不包含stack trace
- report.md包含commit hash、branch名称、测试请求信息
- failure_details.md包含失败测试的完整stack trace
- 格式清晰易读

### TC13: 资源清理 [L2-Agent]
**目标**：验证资源正确清理

**测试步骤**：
1. 完成测试执行和归档
2. 检查worktree是否删除
3. 验证临时文件清理

**预期结果**：
- worktree目录被删除
- `/tmp/claude-tmp/` 下对应目录不存在
- 归档完成后才执行清理

### TC14: 异常处理 [L2-Agent]
**目标**：验证异常情况下的处理

**测试步骤**：
1. 模拟测试中断（Ctrl+C）
2. 模拟编译失败
3. 模拟测试超时

**预期结果**：
- 中断时正确清理资源
- 编译失败时给出明确错误信息
- 超时测试被正确终止并记录

### TC15: 阶段独立验证机制 [L2-Agent]
**目标**：验证每个阶段都有独立的验证agent

**测试步骤**：
1. 检查SKILL.md中每个Phase后是否有独立的Verification section
2. 验证verification agent的指令是否包含具体的检查项
3. 验证不通过时是否有重新执行的逻辑

**预期结果**：
- 每个Phase后有独立的Verification block
- 验证内容具体且可执行
- 验证失败时重新运行执行agent

### TC16: 预执行期望生成 [L2-Agent]
**目标**：验证在测试执行前生成预期内容

**测试步骤**：
1. 检查SKILL.md中是否有预执行期望生成阶段
2. 验证该阶段是否分析测试源码（grep @Test等）
3. 验证是否记录预期测试用例数量

**预期结果**：
- 存在预执行期望生成阶段
- 通过代码分析估算测试用例数
- 后续验证步骤引用预期数据

### TC17: 报告拆分为摘要+详情 [L1-测试]
**目标**：验证报告输出为两个文件

**测试代码概要**：
- generate_report()返回摘要内容（无stack trace）
- generate_failure_details()返回失败详情（含stack trace）
- CLI --output-dir 写两个文件

**预期结果**：
- report.md不包含stack trace
- failure_details.md包含完整失败信息

### TC18: 细粒度表格只列失败项 [L1-测试]
**目标**：验证细粒度统计表格只包含失败的test case

**测试代码概要**：
- 给定混合数据（有成功有失败），细粒度表格只列failed>0的行
- 表格下方有总数summary行

**预期结果**：
- 全部通过的test case不出现在表格中
- 有summary行显示总test case数和失败数

### TC19: commit和测试请求信息在报告中 [L1-测试]
**目标**：验证commit信息和测试请求直接在报告中，不生成单独文件

**测试代码概要**：
- generate_report()输出包含commit hash、branch、test request
- SKILL.md Phase 6不创建commit_info.txt和test_request.txt

**预期结果**：
- 报告包含commit和test request信息
- 归档目录无commit_info.txt和test_request.txt

### TC20: 时间戳格式统一 [L2-Agent]
**目标**：验证所有时间戳使用YYYYMMDD_HHmmss格式

**测试步骤**：
1. 检查SKILL.md中TIMESTAMP定义
2. 检查run_tests.sh中时间戳格式
3. 检查归档目录命名

**预期结果**：
- SKILL.md TIMESTAMP定义为YYYYMMDD_HHmmss
- run_tests.sh使用一致的格式
- 归档目录格式为flink-test-analyzer_YYYYMMDD_HHmmss

### TC21: 归档目录带前缀 [L2-Agent]
**目标**：验证归档目录命名带有flink-test-analyzer前缀

**测试步骤**：
1. 检查SKILL.md中ARCHIVE_DIR定义

**预期结果**：
- ARCHIVE_DIR格式为{PROJECT_ROOT}/log/flink-test-analyzer_{TIMESTAMP}

## 验收标准

1. **功能完整性**：所有核心功能正常工作
2. **错误处理**：异常情况下系统稳定，资源不泄露
3. **报告质量**：生成的报告准确、完整、易读，且可追溯到代码
4. **日志管理**：失败日志正确拆分和归档，便于后续分析
5. **自动化程度**：除必要的用户确认外，全程自动化
