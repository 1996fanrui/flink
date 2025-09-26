## 代码生成规则

请严格遵守以下规则：
- 使用 TDD 开发模型，修改或新增代码前请先完成 test 代码的变更
- 等所有代码修改完且结束 coding 前，请务必运行 mvn 对代码修改相关的代码进行 test ，如果有报错请自动修复，直到报错被解决后才停止。

注意：
1. 一轮代码如果要修改多处代码，不用修改完每处代码都运行 test，等这一轮所有代码都修改完以后让  flink-test-runner sub agent 运行相关 test。避免test 运行次数过多。
2. 不要运行 mvn 全局的 test，当前项目的全局 test 运行极慢，可能会超过 30分钟。
3. 如果 test 运行时报错，且报错是因为 代码 编码导致，则可能执行 ./mvnw clean install -U -Pfast -DskipTests -P java11-target -P java11 进行编译。编译后再次 执行相关的 test。编辑也需要 5分钟以上，所以尽量避免重复编译
4. test 必须由 flink-test-runner sub agent 运行，主 agent 是协调者。不做具体的开发和测试。

注： 需要加  -P java11-target -P java11 后缀来运行 Java 11。
