## 代码生成规则

请严格遵守以下规则：
- 使用 TDD 开发模型，修改或新增代码前请先完成 test 代码的变更
- 等所有代码修改完且结束 coding 前，请务必运行 mvn 对代码修改相关的代码进行 test ，如果有报错请自动修复，直到报错被解决后才停止。
- 代码（生产代码和测试代码）中**禁止**出现任何与需求/设计文档绑定的临时产物。具体包括但不限于：
  - 需求 ID / 验收编号（如 `REQ-NHLB`、`AT-41PK`、`FLINK-38544` 之类的 Jira ticket 引用）
  - `requirements/`、`design.md`、`acceptance_test.md` 等设计文档路径引用
  - 解释"为什么在这次迭代里这样做"、"参考某某文档"之类的过程性叙述
  - 这些内容属于 PR 描述、commit message 和 `requirements/` 目录下的设计文档，写入代码会随着需求演进腐烂，污染长期代码库
- 代码中的 javadoc / 注释只解释"为什么"（非显而易见的约束、不变式、边界条件），**不解释**"这是哪个需求做的"。如果某个不变式需要佐证，请在 PR 描述或 commit message 里链接设计文档，不要在代码里写路径
- 测试代码中**禁止**使用与生产代码对应的 magic number 常量。凡是生产代码里有定义的默认值（如 `MemoryManager.DEFAULT_PAGE_SIZE`），测试必须直接引用该常量，避免测试与生产脱节
- 如果需要偏离默认值测试，用相对表达（`MemoryManager.DEFAULT_PAGE_SIZE * 2` 等）而不是写 `64 * 1024` 这类魔数

注意：
1. 一轮代码如果要修改多处代码，不用修改完每处代码都运行 test，等这一轮所有代码都修改完以后让  flink-test-runner sub agent 运行相关 test。避免test 运行次数过多。
2. 不要运行 mvn 全局的 test，当前项目的全局 test 运行极慢，可能会超过 30分钟。
3. 如果 test 运行时报错，且报错是因为 代码 编码导致，则可能执行 ./mvnw clean install -U -Pfast -DskipTests -P java11-target -P java11 进行编译。编译后再次 执行相关的 test。编辑也需要 5分钟以上，所以尽量避免重复编译
4. test 必须由 flink-test-runner sub agent 运行，主 agent 是协调者。不做具体的开发和测试。

注： 需要加  -P java11-target -P java11 后缀来运行 Java 11。

# Skill 开发原则

## Skill 结构

- Skill 开头定义全局变量（如时间戳、路径、用户输入），后续所有阶段通过变量引用，避免硬编码
- 每个阶段（Phase）包含：执行步骤（委托给 sub agent）、Checklist（产出物清单）、Verification（独立验证）
- 支持条件阶段：在阶段开头明确跳过条件（如"无失败时跳过分析"），避免所有场景执行所有步骤

## 分层架构

- Coordinator agent 是纯协调者，不直接执行 shell 命令或文件操作，所有具体工作通过 Task tool 委托给 sub agent
- 每个 sub agent 接收自包含的 prompt，包含完成任务所需的全部上下文（路径、变量、具体命令），不依赖 coordinator 的隐式状态
- 验证必须由独立的 verification sub agent 执行，不允许执行者自己验证自己的产出

## 确定性操作

- 确定性任务（日志解析、数据去重、报告生成、文件拷贝等）用脚本实现，比 LLM 更快、更准确、更省 token
- LLM agent 仅用于需要理解和判断的任务（如根因分析、自然语言搜索、代码修复建议）
- 脚本输出结构化数据（JSON），供后续阶段的 agent 或脚本消费

## 验证机制

- 每个执行阶段配套 Checklist（产出物清单）和 Verification（独立验证 sub agent）
- Checklist 列出该阶段所有预期产出物，Verification sub agent 逐项检查
- 验证失败后自动重新运行执行 sub agent，而非直接终止
- 阶段间严格串行：当前阶段验证全部通过后才进入下一阶段

## 资源管理

- 使用 git worktree 隔离执行环境，避免污染主工作目录
- 清理保证：即使任务失败也必须清理临时资源（worktree、临时分支），在 Error Handling 中强制执行清理阶段
- 所有产出物归档到项目内持久化目录（如 `log/` 目录下），归档后更新文件内的路径引用指向归档位置

## 错误处理

- 失败先尝试恢复一次再中止，避免因瞬态错误导致整个流程失败
- 明确标识关键失败点：构建失败、不可修复的环境错误等必须立即终止流程，不允许带着错误继续执行
- 任何阶段失败时必须输出失败的阶段名称和错误详情，便于定位问题
