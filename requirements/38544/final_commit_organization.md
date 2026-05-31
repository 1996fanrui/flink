# Final Commit Organization

本文档约束 FLINK-38544 spilling v2 的**最终提交整理阶段**。它只在用户明确要求整理 commits 时使用；修复阶段继续遵循 [Fix Commit 规范](fix_commit_convention.md)，只追加 fixup / docs commit，不改写历史。

## 目标

最终整理后的 FLINK-38544 feature stack 必须按下面顺序排列：

1. 多个文档 commits：保留 `requirements/38544/**` 的历史迭代顺序，整体放在代码 phase commits 之前。
2. 六个代码 phase commits：
   - Phase 0：`[FLINK-38544][network] Decouple LocalInputChannel recovery wiring from toBeConsumedBuffers`
   - Phase 1：`[FLINK-38544][network] Phase 1: common interfaces & sentinels for spilling v2`
   - Phase 2：`[FLINK-38544][network] Phase 2: InputChannel side push-based recovery`
   - Phase 3：`[FLINK-38544][checkpoint] Phase 3: SpillFile + filter writer phase`
   - Phase 4：`[FLINK-38544][checkpoint] Phase 4: spill reader drain + heap fallback removal`
   - Phase 5：`[FLINK-38544][checkpoint] Phase 5: checkpoint 3-step coordination`

最终整理后不应保留独立的 fixup commit、bug-fix commit、debug doc commit。`requirements/38544/**` 的文档 commits 应保留其迭代历史并整体前置；所有 `flink-runtime/**` 改动严格按 `fix_commit_convention.md` 的文件归属规则归并回六个 phase commits。

## 与修复阶段的边界

修复阶段的目标是保留可追溯的增量历史，便于 review 和回滚；最终整理阶段的目标是生成 reviewer 友好的提交栈。两者不能混用。

- 修复阶段：不 rebase、不 amend phase commit；新增 runtime 修复用 `git commit --fixup=<phase-commit>`；新增文档可以作为普通 docs commit 留在分支尾部。
- 最终整理阶段：允许交互式 rebase；把 docs commits 移到六个代码 commit 之前并保留相对顺序；把 runtime fixup / bug-fix / polish commit 合并进对应 phase。
- 如果最终整理后又发生新的 review comment 或 bug fix，先回到修复阶段追加 commit；下一次最终整理时再重新归并。

## 整理前检查

执行最终整理前必须确认：

1. 已经从当前 HEAD 创建临时备份分支；不要只记录 commit hash。
2. 工作区干净。
3. 当前 feature stack 的基点是 Phase 0 的父提交，例如当前为 `904991906e2e0c7e23a35b99e2601a57c619fe64`。
4. 从基点到 HEAD 的最终文件范围只包含 `flink-runtime/**` 和 `requirements/38544/**`。
5. 记录整理前 HEAD commit 和 tree hash；整理后必须能证明最终内容完全一致。

```bash
BACKUP_BRANCH=38544-spilling-v2/backup-before-final-organization-$(date +%Y%m%d-%H%M%S)
git branch "$BACKUP_BRANCH" HEAD
BEFORE_HEAD=$(git rev-parse HEAD)
BEFORE_TREE=$(git rev-parse HEAD^{tree})
printf '%s\n%s\n' "$BEFORE_HEAD" "$BEFORE_TREE"
```

tree hash 代表整个工作树内容。最终整理只允许改变 commit 组织方式，不允许改变任何文件的最终内容；因此整理后的 tree hash 必须与 `BEFORE_TREE` 完全一致。

整理前也可以按需推送备份分支：

```bash
git push origin "$BACKUP_BRANCH"
```

整理后必须使用同一个备份分支和记录的 tree hash 做验收。

如果不使用 shell 变量，至少手动记录下面两个值：

```bash
git rev-parse HEAD
git rev-parse HEAD^{tree}
```

文件范围检查：

```bash
git diff --name-only <base>..HEAD -- ':!flink-runtime/**' ':!requirements/38544/**'
```

该命令必须无输出。若有输出，先判断是否应该保留；不要把范围外文件静默带入最终提交栈。

## 文档 commit 规则

`requirements/38544/**` 是设计、review、修复记录和最终整理规范的工作文档。每次文档更新通常对应一次需求理解、review 结论或修复策略变化，这些历史本身有价值。最终提交栈中，文档 commits 必须整体前置到六个代码 phase commits 之前，但不 squash 成一个最终快照。

文档 commit 规则：

- 每个文档 commit 只包含 `requirements/38544/**`。
- 所有文档 commits 放在 Phase 0 之前、feature stack 基点之后。
- 保留文档 commits 的原始相对顺序，体现设计、review、修复记录的演进过程。
- 禁止把多个有独立语义的文档 commits squash 成一个。
- 只有纯临时、无保留价值、且最终文档中没有任何有效内容依赖的 debug doc commit 可以删除；删除前必须确认其内容已无意义，而不是仅凭 subject 判断。

不要把文档改动合并进代码 phase commit。否则后续看单个代码 commit 时会混入过程文档，降低 phase commit 的可读性。

## Runtime commit 归并规则

所有 `flink-runtime/**` 改动最终只能归入六个 phase commits。归属规则严格复用 [Fix Commit 规范](fix_commit_convention.md) 的“按文件归属 phase”规则：先按文件归属判断；只有已登记的跨阶段文件，才按该文档表格里的语义规则判断。

归属判断顺序：

1. 先用 `fix_commit_convention.md` 里的文件到 phase 映射确定文件归属。
2. 除跨阶段文件表列出的例外外，同一文件只能归入它所属的唯一 phase。
3. 对跨阶段文件，严格使用 `fix_commit_convention.md` 跨阶段文件表中的归属判定准则。
4. 如果整理时发现新的跨阶段文件，先补充 `fix_commit_convention.md` 的跨阶段文件表，再继续整理。

禁止直接按修复 commit subject 猜测 phase。commit subject 只能作为线索，最终以 `fix_commit_convention.md` 的文件归属规则为准。

### Runtime 整理策略

runtime 修复 commit 数量少、且能干净 rebase 时，优先逐个移动到对应 phase 后并标记为 `fixup`。这样能最大限度保留每个修复的来源。

如果后续 runtime commits 数量多、互相反复修改同一批文件、或在交互式 rebase 中产生大量冲突，可以先把这些后续 runtime 改动合并成一个临时 runtime 汇总 commit，再按 `fix_commit_convention.md` 的文件归属规则拆回六个 phase commits。这个临时 commit 只是降低冲突成本的整理中间态，不允许留在最终提交栈。

采用临时 runtime 汇总策略时必须满足：

- 先保留并前置文档 commits，不把文档混进 runtime 汇总。
- 临时 runtime 汇总只包含 `flink-runtime/**`。
- 汇总前后 tree hash 必须一致。
- 拆回 phase 时仍按文件归属和跨阶段文件表归属，不允许因为汇总过就把所有改动塞进一个 phase。
- 最终提交栈中不能留下这个临时 runtime 汇总 commit。

## 交互式 rebase 规则

最终整理推荐使用交互式 rebase，不直接 amend 现有 commits。

```bash
git rebase -i <base>
```

todo list 应调整为：

1. 所有 `requirements/38544/**` docs commits 放在最前，保留相对顺序，不 squash 成一个文档 commit。
2. 保留六个 phase commits 的原始顺序。
3. 所有 `fixup! ... Phase N ...` commit 放到对应 phase commit 后并标记为 `fixup`。
4. 所有不是 `fixup!` 但只修改 `flink-runtime/**` 的修复 commit，先按文件归属规则判定 phase；若冲突可控，移动到目标 phase 后并标记为 `fixup`。
5. 如果 runtime 修复 commit 冲突过多，先合并为一个临时 runtime 汇总 commit，再按文件归属规则拆分并归并到六个 phase commits。
6. 删除只用于临时调试且不应保留最终内容的 commit；如果 commit 中有仍需保留的文档或 runtime 改动，必须先拆分归位，不能直接丢弃。

禁止事项：

- 禁止把 runtime 修复留在六个 phase commits 之后。
- 禁止把 docs commit 留在六个 phase commits 之后。
- 禁止 squash 有独立语义的文档 commits。
- 禁止用 commit subject 代替 diff 审查。
- 禁止因为冲突多就保留独立修复 commit。

## 冲突处理

冲突处理以“整理后 tree 与整理前 HEAD tree 一致”为最高准则。

- 对文档冲突：取最终 HEAD 中 `requirements/38544/**` 的内容。
- 对 runtime 冲突：取最终 HEAD 中对应文件的内容，但必须确认该内容归入了正确 phase。
- 对跨 phase 文件冲突：先按 `fix_commit_convention.md` 的跨阶段文件表确认冲突区域归属哪个 phase，再解决；不要为了快速过 rebase 把整文件错误地归到一个 phase。

如果冲突处理过程中需要临时查看整理前内容，使用备份分支或记录的原始 HEAD，不从记忆恢复。

## 整理后验收

最终整理完成后必须检查：

1. tree hash 与整理前记录的 `BEFORE_TREE` 完全一致。
2. `git diff <backup-branch>..HEAD` 无输出。若有输出，说明最终内容变了，必须停止并修复；最终整理默认不允许“顺手改代码/文档”。
3. `git diff --name-only <base>..HEAD -- ':!flink-runtime/**' ':!requirements/38544/**'` 无输出。
4. 从基点到 HEAD 的 feature stack 只剩多个前置 docs commits 加六个 phase commits。
5. 工作区干净。

代码/文档内容一致性检查：

```bash
test "$(git rev-parse HEAD^{tree})" = "$BEFORE_TREE"
git diff --exit-code "$BACKUP_BRANCH"..HEAD
```

这两个命令都必须成功。最终整理的交付标准是：**只改变 commit history，不改变最终文件内容**。

提交结构检查示例：

```bash
git log --oneline --reverse <base>..HEAD
```

预期形态：

```text
[FLINK-38544][docs] ...
[FLINK-38544][docs] ...
[FLINK-38544][network] Decouple LocalInputChannel recovery wiring from toBeConsumedBuffers
[FLINK-38544][network] Phase 1: common interfaces & sentinels for spilling v2
[FLINK-38544][network] Phase 2: InputChannel side push-based recovery
[FLINK-38544][checkpoint] Phase 3: SpillFile + filter writer phase
[FLINK-38544][checkpoint] Phase 4: spill reader drain + heap fallback removal
[FLINK-38544][checkpoint] Phase 5: checkpoint 3-step coordination
```

若最终整理改动了代码内容而不是只改历史，必须停止并重新确认原因；最终整理本身不应改变工作树最终内容。
