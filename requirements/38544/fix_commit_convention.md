# Fix Commit 规范

当前分支已按 Phase 1–5（含一个 Phase 0 前置 refactor）拆分为多个独立 commit。后续所有修复阶段产生的 commit 统一称为 **fix commit**，遵循以下规范，便于后续 squash / rebase 时按 phase 合并。

## 总体原则

修复期所有 commit **必须**满足：

1. 归属唯一一个 phase。
2. 用 `git commit --fixup=<target-phase-commit>` 形式生成（不允许自由 subject）。
3. 显式列出待提交文件（`git add <paths>`），不允许 `git add .` / `git add -A`。
4. 不允许 amend 已有 phase commit；不允许跨 phase 改动。
5. 不允许引入新单元 / 集成测试。仅在接口签名变更时同步修改现有测试。

## 规则 1：一个 fix commit 只归属一个 phase

修复任何问题前，先判定该问题属于哪个 phase（Phase 0 / 1 / 2 / 3 / 4 / 5），然后为该 phase 单独创建一个 fix commit。一个 fix commit 不允许同时修改多个 phase 的代码。

## 规则 2：commit 形式 —— 必须是 fixup commit

**强制使用 `--fixup` 形式**：

```
git commit --fixup=<phase-N-commit-hash>
```

这会生成 subject 为 `fixup! <被 fixup commit 的 subject>` 的 commit，后续可通过 `git rebase -i --autosquash <base>` 自动按对应 phase 合并。

**禁止使用** 手写的 `[FLINK-38544][fix][phaseN] <简述>` 自由 subject —— 那种 subject 不能被 `git rebase --autosquash` 识别，squash 时需要人工编辑 todo list，容易出错。

如何获取 phase commit hash：

```
git log --oneline 159560fd730..HEAD | grep -v fixup
```

## 规则 3：按文件归属 phase

所有文件按其所属 phase 归类；**跨阶段文件**按本次改动的**语义**决定归属哪个 phase commit，**不按文件历史归属死规定**。

### 当前已知的跨阶段文件清单

| 文件 | 改动过的 phase | 归属判定准则 |
|------|--------------|------------|
| `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/RecoveredInputChannel.java` | Phase 1 / 2 / 4 | 按本次改动的字段、方法、行号语义归属 |
| `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/LocalInputChannel.java` | Phase 0（解耦 refactor）、Phase 2（接入 RecoverableInputChannel 接口） | 注释级改动 → phase 0；功能/接口/语义改动 → phase 2 |
| `flink-runtime/src/main/java/org/apache/flink/runtime/io/network/partition/consumer/BufferManager.java`、`.../netty/NettyMessage.java`(PartitionRequest)、`.../netty/CreditBasedSequenceNumberingViewReader.java`、`.../netty/NettyPartitionRequestClient.java`、`.../netty/PartitionRequestServerHandler.java` 及其对应 test | spilling-v2 原 6 个 phase commit **从未碰过**（master 既有文件），首次改动来自 recovery-phase credit 超发修复 | **跟随 `RemoteInputChannel`** → Phase 2。判定准则：这些改动都是为 `RemoteInputChannel` 的 recovery credit 流控服务的（`BufferManager` 是其成员、netty `PartitionRequest`/reader/handler 是其上游对端协议），`RemoteInputChannel.java` 的 recovery 改动在 Phase 2 引入，故同归 Phase 2 |

后续如发现新的跨阶段文件，**必须先把它追加到这张表里**，再开始改动。

### 归属判定的判断方法

1. 改的是哪个 phase 引入的代码？— 若新增方法体或字段 → 归引入它的 phase。
2. 改的是注释 / 格式 / 行号？— 注释规范化按行号 blame 归属（默认归 phase 0 的文件就归 phase 0）。
3. 改的是接口契约 / 函数语义 / 数据流？— 这个改动是哪个 phase 的功能落地的一部分？归对应 phase。
4. 实在判不清 → 在 review doc 里讨论，写定后追加到上方表格。

除上述跨阶段文件外，所有其他文件的改动只允许归到其所在的唯一 phase。

文件 → phase 映射可通过下面命令在本分支当前状态下查询：

```
for c in 129d7b3 b892c23 7cff232 4121ded 4ebb644 0b90235; do
  echo "=== $c ==="
  git diff-tree --no-commit-id --name-only -r $c
done
```

（commit hash 在每次 rebase 后会变，需要重新查；按 commit subject 中的 phase 标签识别也可）

## 规则 4：修复期不引入新测试

当前主流程尚未跑通，大量新增测试可能基于错误前提。修复期遵循：

- 保留现有测试，确保能跑
- 接口 / 签名变更时，仅修改对应的现有测试
- 不新增单元测试或集成测试
- 主流程跑通后再统一回补测试

## 规则 5：显式 stage 文件

`git add` 必须显式列出本次 fix 涉及的文件路径，禁止 `git add .` / `git add -A` / `git add -u`。原因：分支工作树常常残留 review doc、临时手动改动等不属于本次 fix 的内容，无差别 add 会把它们误带进 commit。

```
# 正确
git add flink-runtime/.../SpillFile.java flink-runtime/.../DiskSnapshot.java

# 禁止
git add .
```

## 规则 6：禁止 amend 与历史改写

phase commit 自身**绝对不允许 amend**。所有修复都必须以新 fix commit 形式追加。理由：
- amend 会改写 phase commit hash，影响所有引用该 commit 的 fixup commit；
- amend 会丢失修复的可追溯历史；
- 后续 squash 阶段才统一改写历史，修复期间保持 phase commit 不变。

---

## 两种修复场景

### 场景 A：用户报告一个**通用问题**（覆盖整个分支多文件）

例：「分支里所有代码不允许用全限定名」「凡是代码没变但注释被删的都要恢复」「凡是 caller 误持有 Chunk.data 的位置都要修」。

#### 适用判定

只要满足下面任一条，就按"通用问题"处理：

- 用户的描述用了"所有"/"凡是"/"统一"/"全部" 等量词；
- 问题本身的根因不局限在某一处代码（如规范、统一约定）；
- 你已经在某处发现该问题，但合理推断分支上其他文件也可能有同类问题。

#### 执行流程

1. **明确分支基**：当前分支基是 `159560fd730`（FLINK-38544 spilling v2 开发分支基），下面所有命令的 base 都用它。
2. **列出整个分支累积改动**：
   ```
   git diff --name-only 159560fd730 HEAD
   ```
   得到分支上所有改动过的文件（包含 6 个原 phase commit + 所有 fixup commit 累积后的状态）。
3. **按"分支基 vs HEAD"做检查**，不是看单个 commit 的 diff。原因：phase commit 历史是开发阶段的快照，之前轮的注释规范化 / FQN 修复已经叠加到 HEAD，必须看最终状态。
4. **逐文件按最终 HEAD 状态扫描问题**。如果问题需要对比 base 才能识别（如"误删注释"），则 `git show 159560fd730:<path>` 拿 base 版本对比。
5. **按 phase 切分修复**：每个文件按其归属 phase 分组，每组生成一个 fixup commit 指向对应的原 phase commit。最多产出 **6 个 fixup commit**（phase 0 ~ phase 5）。
6. **每个 fixup commit 内部**：
   - 显式 `git add` 该 phase 名下的所有文件。
   - 用 `git commit --fixup=<phase-N-commit-hash>` 提交。
   - 不跑 spotless 期间不要 commit；最后统一 spotless apply 后再 commit。
7. 修复完成后跑 `mvn compile test-compile -pl flink-runtime -P java11-target -P java11` 确认编译通过；如改动可能影响行为，跑相关已有测试。

#### 委托给 sub agent 时的注意点

- 必须给 sub agent 强限制：只能改属于该 phase 的文件清单（直接给绝对路径列表）；
- 跨 phase 例外文件（如 `RecoveredInputChannel.java`）按行号段分给对应 phase；
- worker 不许跑 spotless / git / mvn / 测试，只做工作树编辑；
- 多个 worker 之间**串行**执行，避免跨文件影响（同文件不被多个 worker 改通常无所谓，但 phase commit hash 在每次 fixup 之后会变化，需要重新获取）。

### 场景 B：用户报告一个**具体问题**（单点 / 局部）

例：「某方法实现有问题，修一下」「某文件的某段注释错了」「这个变量名应改」。

#### 执行流程

1. 定位问题代码（哪个文件、哪个方法 / 字段 / 段）。
2. 判定该文件归属哪个 phase。
3. 修改代码 → 跑 spotless → 跑相关测试。
4. **显式** `git add <修改的文件>`。
5. **找到该 phase 的原始 commit hash**：
   ```
   git log --oneline 159560fd730..HEAD | grep -v fixup
   ```
   按 subject 里的 `Phase N` 标签确认。
6. `git commit --fixup=<phase-N-commit-hash>`。
7. 工作树里如果还有其他未提交的脏文件（手动改动、review doc），**留在工作树**，不要带进 commit。提交前再次 `git status --short` 确认。

---

## 操作 checklist（每次修复前）

无论场景 A 还是场景 B，提交前都要核对：

1. ✅ 问题归属 phase 已判定（0 / 1 / 2 / 3 / 4 / 5）。
2. ✅ 仅改动该 phase 范围内的文件（`RecoveredInputChannel.java` 按改动语义归属）。
3. ✅ 仅在接口变化时同步修改现有测试，不新增测试。
4. ✅ 已跑 `mvn spotless:apply -pl flink-runtime -P java11-target -P java11`。
5. ✅ `git status --short` 查看，确认工作树没有不相关的脏文件被带进 stage。
6. ✅ 用 `git add <paths>` 显式列出本次提交文件。
7. ✅ 用 `git commit --fixup=<phase-N-commit-hash>` 提交。
8. ✅ 不 `git push` / 不 rebase / 不 amend 既有 commit。

## 最终 squash 阶段（仅在用户明确同意时执行）

```
git rebase -i --autosquash 159560fd730
```

执行前必备：
- 当前分支状态 backup 到 tag 或 backup 分支（如 `38544-spilling-v2/backup-before-squash`）；
- 记下 HEAD tree hash，squash 后必须验证 tree hash 完全一致；
- 跨 phase 文件（`RecoveredInputChannel.java` / `LocalInputChannel.java`）的 fixup 可能因为 phase commit 当时的代码状态与 fix 写出来时的代码状态不同而冲突。冲突处理：取 HEAD 上该文件应有的最终内容作为冲突解决方案。
