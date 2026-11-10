# MaiBot 人格与表达素材

本项目保留“大象”名称和 Elephant-Bot 项目名称。默认人格综合 Aurabot 提交 `1057a861e4e96aa2fe050000d238280a7901b9dc` 的角色设定与手机聊天习惯，以及 MaiBot 提交 `e5391507531706a8bfce011d55ed94628039f2de` 的分层人格、口语表达和群私聊习惯。表达学习沿用 MaiBot 的默认快速表达机制。来源路径、SHA256、许可和适配记录见 [第三方说明](../THIRD_PARTY_NOTICES.md) 与 [上游锁定清单](../upstreams.lock.yaml)。

## 人格配置

默认文件为 `personas/elephant_default.yaml`，版本为 6。角色保留蓝色长卷发、鲸鱼耳朵和尾巴、白米饭偏好，服装采用用户定制的灰白女仆装和大象围裙。日常语气平实随意，允许省略主语、短句和少量语气词，任务说明按需要展开。角色设定、交流习惯和场景提示分别维护。

| 字段 | 用途 |
| --- | --- |
| `name` | 默认“大象”，会话名称覆盖由渲染器统一应用 |
| `identity` | 角色背景、外貌和喜好。输入兼容 MaiBot 的 `personality` 别名，保存统一使用 `identity` |
| `traits` | 稳定性格，包含自主判断、好奇、轻吐槽和关心对方 |
| `behavior_style` | 如何接话、回应感受和说明任务情况，始终服从发言许可与工具权限 |
| `reply_style` | 句式、措辞、日常篇幅及少量语气示例，示例按实际情境使用 |
| `group_chat_prompt` | 多人话题、公开问答、排障补充、接梗与主动参与限制 |
| `private_chat_prompts` | 私聊节奏、亲近程度及给对方留空间，沿用 MaiBot 的复数键名 |
| `style` | 默认语言与正式程度等少量附加偏好 |
| `multiple_reply_style` | 语气温度接近的备用风格字符串数组 |
| `multiple_probability` | 每个执行 run 触发备用风格的概率，范围 0～1，默认 0 |
| `boundaries` | 隐私、事实、身份和权限边界 |

普通聊天、主动搭话、任务进度、追问、失败说明和收尾共用人格渲染与宿主表达约定。群聊与私聊分别选取注意事项，备用风格在每个执行 run 或每次主动搭话生成时只抽取一次。权限、事实边界和宿主输出约定优先，会话表达覆盖优先于全局默认，临时风格和表达素材仅供当前情境参考。日常短回复要求服从当轮内容的完整性需要。SpeechPolicy 仍负责发言许可与参与频率。

人格正文进入可信系统提示词，Worker 发送给 Pi 的背景 seed 省去重复人格，宿主持久快照和版本校验保留。历史、记忆和逐轮表达素材继续放在资料区。

群聊偏好沿用用户指定的参与范围。公开问题需已有可靠信息或可用的公开了解途径，排障补充需有关键遗漏，闲聊可接梗或复读。涉及未公开的个人、学校或内部信息时不主动回答，政治、争论及健康等严肃话题不主动加入。

管理页保存的全局人格位于数据库目录的 `persona-global.json`，其 `source_path` 与当前默认 YAML 路径匹配时优先使用保存内容。部署或更新 YAML 后，已有全局覆盖仍需在管理页单独更新。管理页保存供后续回合使用，直接编辑 YAML 后应重启 Core。会话覆盖独立保存在 `persona-overrides.json`，允许 `name`、`formality`、`reply_length` 和 `participation`。保存会替换该会话的整个覆盖对象，空对象恢复全局偏好。

## 表达从哪里来

表达素材是“当某种情况出现时，可以采用某种措辞”的记录，不是事实记忆，也不是固定回复库。上游学习提示词与审核提示词保留原文，并复用其表达规范化、示例过滤和加权抽样代码。

1. Core 后台循环约每 30 秒扫描会话；每个会话还受学习间隔和最少消息数限制。默认积累至少 10 条有效消息，每批最多 40 条，学习间隔 300 秒。
2. 只从允许范围的人类文字消息提取，不学习机器人自身、命令、被拉黑来源、已撤回或已清理来源。模型输出必须引用本批有效来源编号；场景与表达分别最多 20 字，每批至多 10 条建议。
3. 默认开启 `expression_self_reflect`：写入前逐条调用模型检查情境匹配、口语可用性、通用性及具体名称等。模型审核通过不等于人工精选；人工状态独立保留。
4. 相同会话、相同场景和表达合并，新增独立来源时增加 `count`；同一来源重放不重复计数。素材、来源、检查点和失败状态保存在 Core 数据库中，重启可继续积累；模型失败不把本批当作成功学习。
5. 每轮生成按当前作用域、权限、来源有效性及人工精选选项过滤素材。可用素材不足 10 条时不抽样，这是上游快速档的冷启动规则，和“至少 10 条消息才学习”是两个不同门槛。
6. 可用素材达到 10 条后，从全池加权抽取最多 5 条；当 `count > 1` 的高频素材也达到 10 条时，再从高频池抽取最多 5 条，合并去重。抽样权重按当前候选的计数线性映射到 1～5，使用不放回抽样。
7. 所选素材以“你可以视情况自由使用以下句式”作为本轮用户数据传给 Pi。它不进入可信系统人格，不提供事实、身份或操作授权；回复模型自行决定是否适合使用。持久会话每一轮都携带本轮素材，不依赖仅首次注入的历史 seed。

安装时不预填虚构的“已学表达”，也不导入其他群或旧 Koishi 的表达库。新库在冷启动期间只使用人格配置说话；表达学习与 `memory.maintenance_enabled` / `memory.auto_extract` 独立。

## 表达配置

以下是默认快速档的主要配置，放在 Core 配置的 `expression` 下：

```yaml
expression:
  enabled: true
  expression_checked_only: false
  expression_self_reflect: true
  learning_list:
    - type: group
      use: true
      learn: true
  expression_groups: []
  learning_blacklist: []
  learner_endpoint: null
  learner_model: null
  learner_key_env: QQBOT_EXPRESSION_KEY
  timeout_seconds: 300
  min_messages: 10
  max_messages: 40
  learn_interval_seconds: 300
  max_expression_learner: 3
```

`learner_endpoint` / `learner_model` 未填写时复用 Core 的 `model.endpoint` / `model.model`；密钥优先读取 `learner_key_env`，使用主模型地址时才允许回退到 `model.api_key_env`。自定义不同地址必须配置表达专用密钥。这些环境变量必须注入 Core，不是仅存在于 Worker。没有可用 endpoint、model 或密钥时，学习明确显示跳过；已有合法素材仍可按配置使用。开启自反审核会增加模型请求，`timeout_seconds` 限制整个提取与自反批次。

`learning_list` 中每条规则包含 `conversation_id`、`adapter_instance_id`、`type`、`use`、`learn`。两个 ID 留空表示该聊天类型的默认规则；指定完整 `conversation_id` 的规则优先于仅指定接入，具体度相同的后写规则优先。不支持 `*` 通配。`use` 控制是否使用已有素材，`learn` 控制是否继续学习；可以暂停学习而继续使用。

默认只对群聊启用学习与使用，私聊必须添加明确的 `type: private` 规则。会话 ID 使用 `/api/v1/conversations` 返回的完整 `id`，不是裸 QQ 群号。`expression_groups` 是完整会话 ID 的数组分组，仅允许显式配置的非私聊会话共享素材；私聊素材不会跨会话使用。共享仍需满足来源和读取权限。

`learning_blacklist` 每项必须包含 `adapter_instance_id`、`identity_namespace`、`user_id` 三个精确身份字段。被排除用户的发言不作为表达来源。`expression_checked_only: true` 时，仅使用已由管理员审核的素材；少于 10 条可用精选素材时仍不抽样。

表达配置通过配置中心保存后需要重启 Core；保存不会自动重启。新建模型专用凭据或更换环境变量值也需使 Core 重新加载环境。不要用人格页的即时保存替代表达配置发布。

学习与自反审核对 DeepSeek V4 / `deepseek-flash` 显式开启思考，强度为 `low`。提取输出预算为 8192 tokens，单条自反审核为 4096 tokens，包含思考消耗；HTTP 响应体上限为 1 MiB。整个提取与自反批次默认限时 300 秒，`timeout_seconds` 可设为 1～1800 秒。空响应、输出截断和格式错误均保留失败批次，不视为成功学习。

## 审核、编辑与删除

管理 API 需要现有管理员认证，以及表达读取/管理权限；先读取记录取得当前 `revision`，再提交修改。版本冲突返回 409，应重新读取后处理，不应盲目覆盖。

| 请求 | 参数与结果 |
| --- | --- |
| `GET /api/v1/expressions` | 查询参数 `conversation_id`；返回 `records`、`learning`、`learner_configured` |
| `PATCH /api/v1/expressions/{id}` | 查询参数 `conversation_id`；JSON 为 `expected_revision`、`checked`，以及可选 `situation`、`style` |
| `DELETE /api/v1/expressions/{id}` | 查询参数 `conversation_id`、`expected_revision` |

`learning` 显示学习/使用开关、积累消息数、游标、状态、失败原因和下次尝试时间；`learner_configured` 只表示配置齐全，不证明供应商调用成功。取消人工精选不会在 `expression_checked_only: false` 时禁止使用该条；要停止使用应删除该条或收紧使用规则。

删除素材会写入删除墓碑并清空正文，避免重放相同素材而复活。来源消息撤回、遗忘或保留期清理会使关联素材失效；生成与发送前会再次检查被选素材的版本和来源。恢复数据库必须同时应用最新删除台账，台账包含表达墓碑，不能只还原旧数据库；具体命令沿用 [运维与恢复](runbooks/operations.md)。逻辑删除不等同于清除旧备份或磁盘扇区。

## 实现范围与验收

此次引入的是 MaiBot 默认快速表达档：完整的提取、自反审核、去重计数、持久恢复、加权抽样和逐轮注入。未引入可选的向量召回与额外 LLM 表达筛选、上游完整 Planner/Replyer 框架、A_memorix、插件 Hook、表情包或多模态运行时；人格中的社交描述不保证具备上游全部能力。

本地验证应分别覆盖人格渲染、表达来源与作用域、模型失败、学习重放、审核冲突、撤回删除/备份恢复，以及 Pi 持续会话的逐轮注入。真实供应商调用和 QQ 客户端语气验收单独记录，不能由配置状态或协议夹具通过替代。该文档不表示已经完成线上部署或真实对话验收。
