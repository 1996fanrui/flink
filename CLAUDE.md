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
