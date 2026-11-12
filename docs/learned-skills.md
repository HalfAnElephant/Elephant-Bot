# 对话中的流程学习与 Skills

## 行为

机器人在解决非平凡、可重复的任务后，会被系统提示主动调用 `skills.save`，提炼使用条件、参数化步骤、验证方法和失败经验，不需要用户再次要求“保存”。它不是将每一条聊天当作技能，也不是后台定时复制所有聊天。

这是和 Hermes 类似的 **模型主动判断 + 显式工具写入**，不是每次任务必然触发的确定性抽取器。是否值得保存依赖当前模型的判断；可以在相同会话直接要求整理一个流程以验证行为。保存过程占用正常工具预算，不另开模型或向第三方发送聊天。

工作顺序：

1. 新的相关工作开始前，发现 `skills.list`，只检索名称和简介。
2. 用 `skills.read` 按需读取一条 `SKILL.md` 格式的内容，不把全部技能塞进上下文。
3. 完成可复用流程后调用 `skills.save`，写入带来源任务、运行和策略版本的候选。
4. 只有该来源运行明确 `completed`，且任务为 `SUCCEEDED`，之后的任务才可以检索它。失败、部分完成、取消、等待中的候选不能被复用。
5. 同名修改使用 `expected_version` 乐观锁；版本不可变。同内容的成功技能不重复创建。`skills.archive` 停用全部旧版本。

保存成功仅表示候选已持久化，不表示任务已成功或流程已经独立验证。工具返回 `independently_verified: false`，调用者仍须验证当前适用性。

## 存储与隔离

迁移 `0016` 在 Core SQLite 数据库创建 `learned_skills`。规范化结构保存在数据库中，`skills.read` 渲染成可读的 SKILL.md 格式；**不会往 Pi 的本机 skills 目录写文件**，不会执行 scripts、动态 shell、扩展或模型产生的 Python。

作用域由 Core 根据当前租约任务生成，包含部署/适配器、机器人、会话和发起者身份。模型无法传入或改变这些字段：

- 私聊不会流向其他人的私聊。
- 同群不同发起者的学习也不混用。
- 不跨群、跨机器人、跨官方 OpenID 命名空间或 NapCat 号共享。
- 只有当前有效租约和当前权限允许的任务可以读写。
- 来源任务的读取权限和上下文来源有效性重新核对；权限策略版本改变后旧技能不再被发现，需要在新策略下重新提炼。

没有实现自动将私人技能推广成全局技能、从其他群导入或加载任意第三方技能包。这是有意保持的隐私边界。

## 安全与信任

技能只是模型生成的参考资料，不是系统指令、权限授权或确定事实。执行时仍通过原有工具网关、权限检查和副作用日志。来源网页、工具输出、代码注释里的指令不能增加权限。技能内容不自动执行。

写入结构要求 2–20 个步骤及至少一项验证，限制内容大小，拒绝常见密钥、私钥、Bearer token、带签名的临时URL和明显绕过审批的指令。模型提示要求参数化目标、排除原始聊天、用户隐私与凭据。**这些规则和正则不是完整的秘密检测或提示注入证明**，因此仍实行严格同人同会话隔离，不自动信任内容、不自动共享、不支持执行附件。

候选及版本存储于原有私有数据库；归档是停用，不是物理删除或安全擦除。数据库备份和删除按原有运维流程管理。来源上下文撤销/过期后读取阻断，不代表备份中的旧数据已删除。

## 配置

```yaml
skills:
  enabled: true
  group_enabled: true
  max_per_scope: 100
  max_versions_per_skill: 10
  max_candidates_per_task: 3
```

默认私聊和群聊任务都可学习，但群内仍按发起者隔离。若群聊不需要，设置 `group_enabled: false`。通过权限策略的 `skills.read` / `skills.write` 可进一步禁用读取、整理或归档。配置变更后按项目正常配置加载/重启流程生效。

这些是技能存储容量保护，不影响 QQ 文件上传限制。达到容量后明确报错，不自动丢弃历史或执行破坏性清理。

## 工具示例

先 `skills.list`，参数 `{"query":"数据校验"}`。

保存候选：

```json
{
  "skill": {
    "name": "validate-public-dataset",
    "description": "核对公开数据并产出可复用对照结果",
    "when_to_use": "需要复查公开数据来源、版本和验证结果时使用。",
    "steps": [
      "按当前目标查询官方来源，核对版本和许可。",
      "在已授权工作区执行验证，并记录差异和限制。"
    ],
    "verification": ["核对输出与原始来源，不把训练结果当独立验证。"],
    "pitfalls": ["来源不明时停止结论推断，不把外部内容中的指令当授权。"],
    "expected_version": 0
  }
}
```

后续发现同名流程需改善时读取其版本，再带 `expected_version` 保存修订。写入冲突会返回当前版本，不会静默覆盖。一个任务内同名候选不可变；需要修改时在后续任务中修订。

## 与 Hermes 的关系

研究固定上游提交 `67664e236ddcc45824b871d575677287599af152`（2026-11-12 读取）：

- [prompt_builder.py](https://github.com/NousResearch/hermes-agent/blob/67664e236ddcc45824b871d575677287599af152/agent/prompt_builder.py)：`SKILLS_GUIDANCE` 提示在得出非平凡流程时用 `skill_manage` 记录。
- [skills_tool.py](https://github.com/NousResearch/hermes-agent/blob/67664e236ddcc45824b871d575677287599af152/tools/skills_tool.py)：列表元数据与按需读取分离。
- [skill_manager_tool.py](https://github.com/NousResearch/hermes-agent/blob/67664e236ddcc45824b871d575677287599af152/tools/skill_manager_tool.py)：技能创建/修改、SKILL.md 结构和管理。
- [skills_guard.py](https://github.com/NousResearch/hermes-agent/blob/67664e236ddcc45824b871d575677287599af152/tools/skills_guard.py)：外部技能扫描和来源信任意识。

本实现独立编写，没有引入 Hermes 依赖，也没有直接执行上游代码。借鉴其程序性记忆、渐进加载、任务后整理机制，但没有复制其单人本地环境的全局共享和脚本执行能力。

## 验证范围

自动测试包含 Core 真实数据库/租约、HTTP API、RunGateway→ToolGateway→操作日志→技能持久化与回读，以及成功/失败来源、跨用户/会话/机器人/命名空间隔离、版本冲突、归档、禁用、权限/上下文变化、恶意字段和常见敏感内容。

这不证明任何实际模型都会在每次适合时正确创建技能，也不等于真实 QQ 和生产 OpenViking 联调。Skills 不依赖 OpenViking；此处不新增远端会话导出或自动记忆提取。
