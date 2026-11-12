# Hermes 技能与自动学习

技能正文采用标准 `skills/[category/]<name>/SKILL.md`，附件位于 `references/`、`templates/`、`scripts/` 或 `assets/`。文件是唯一可写正文，SQLite 的旧 `learned_skills` 表只用于迁移和历史审计。

Leader 与普通 Worker 使用同一组工具，宿主根据已验证的 owner 绑定目录。`skills_list` 返回名称、描述、分类；`skill_view(name, file_path?)` 按需读取正文或附件；`skill_manage` 使用上游原生 `operations` 数组直接创建或修改文件，无候选、任务成功激活或 Leader 审批阶段。技能正文不会改变宿主授权。

```json
{"operations":[{"action":"patch","name":"monthly-report","old_string":"导出后发送。","new_string":"导出后核对日期与合计，再按当前授权发送。"}]}
```

优先读取并修正已有技能，只有没有合适归属时才新建。`create` 提供完整 frontmatter 和非空正文；新建或改变的目录描述上限为 60 字符。`patch` 使用 `old_string/new_string`，也支持以 `content` 完整重写。`write_file/remove_file` 管理相对附件路径；`delete` 必须是单独一项。批处理持有所有相关技能锁，失败会尝试恢复修改前快照；回退失败保留恢复材料并明确报错。批处理不承诺数据库式崩溃原子性。

每次变更尽力记录 `.curator_ledger.jsonl` 和内容寻址备份。`HermesSkills.ledger()` 可查询，`rollback(entry_id)` 使用上游回退并持有同一技能锁；无法完成安全快照时拒绝回退。ledger 失败不阻止保存，因此成功保存不等于必有可回滚版本。

配置示例：

```yaml
skills:
  enabled: true
  group_enabled: false
  provider: hermes
  owner_storage_root: /var/lib/elephant/owners/personal-owner
  background_review_enabled: true
  hermes:
    write_approval: false
    inline_shell: false
    ledger: true
    creation_nudge_interval: 10
```

`create_dir` 若配置，必须与宿主绑定的 owner 根目录下的 `skills` 一致。其他 owner 的发现根、锁、ledger 和备份互相隔离。运行时不扫描外部 profile、项目或插件技能，不执行技能内联 shell，不自动安装依赖；脚本只能通过正常受控执行工具运行。Pi 原生技能扫描应关闭，统一使用这三个工具。

学习钩子为 `AgentLearningState`：按实际 Agent Session 保存 `dataclasses.asdict(state)`，每次真实工具循环调用 `note_iteration()`，工具调用时调用 `note_tool(name)`；`skill_manage` 重置计数。回合持久完成后调用 `finish_turn(final_response=..., interrupted=..., is_review=...)`，满足上游阈值和条件时返回一次受限复盘请求。宿主将其持久排入统一准入队列，使用相同 Harness，工具仅限三个技能工具，预算最多 16 轮。复盘读取快照，不回写原 Session；完成后调用 `finish_review()`。它不持有原对话写者，也不递归触发学习。

只有该复盘执行使用 `adapter.review_context()`；普通异步 Worker 仍为 foreground。分散的工具 HTTP 调用可通过同一个宿主保存的 read marks 对象恢复 `review_context(marks)`，强制本次复盘先读后改。复盘不允许删除技能；主任务结果、技能保存和通知投递分别记录。

迁移前冻结旧写入口。`qqbot.skills.migration.migrate_legacy_skills(db, provider_for_scope)` 接受宿主核实过的旧 scope 到 owner 映射，只导入仍有效且成功源任务的最新技能。归档、失败、未绑定范围及历史版本保留状态和正文哈希清单，不自动混入 owner 技能；同名冲突使用稳定后缀，不覆盖现有文件。重复执行核对同内容，不重复创建。迁移后旧接口和旧配额配置明确报迁移错误。

采用的上游提交、原文件哈希、本地适配和完整 MIT 许可见 `upstreams.lock.yaml` 及 `src/qqbot/adapters/hermes_skills/`。Curator 的定时维护不在首期启用范围。
