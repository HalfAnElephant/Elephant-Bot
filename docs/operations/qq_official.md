# QQ 官方富消息接入

官方适配器支持按实例选择 Webhook 或 WebSocket。扫码自动接入使用官方长连接。能力清单记录代码与协议测试覆盖，平台实际权限和客户端验收单独记录。

## 能力清单

| 功能 | QQ 私聊 | QQ 群 | 频道 / 频道私信 |
|---|---|---|---|
| 官方文本接收与回复 | 是 | 是，全部消息需授权 | 是 |
| 自定义 Markdown | 是 | 是 | 已编码，待平台验收 |
| 模板 Markdown / 消息键盘 | 是 | 是 | 已编码，待平台验收 |
| 图片 / 音频 / 视频 / 文件 URL 上传发送 | 是 | 是 | 仅 URL 图片，其他拒绝 |
| 图文卡片 | 是 | 是 | 当前显式拒绝 |
| 引用 | 保留并可发送官方引用索引 | 同左 | 文本引用仍需实测 |
| 组合、转发、Ark 入站 | 保留有界原始结构 | 同左 | 不承诺可发送 |
| 输入中 | 可配置启用 | 无同等实现 | 无同等实现 |
| 原生流式 | 可配置启用 | 最终回复降级 | 最终回复降级 |
| 按钮回调 | 验签、ACK、作用域令牌、状态/帮助动作 | 同左 | 解析，发送能力单独限制 |
| 菜单 / 指令面板 | 管理 API | 面板按官方 scope | 面板按官方 scope |
| 扫码配置应用 | 管理页面自动绑定 owner、配置权限并连接，支持手动保存 | 同一应用 | 按 intents 与实际权限启用 |

## 配置与启用

支持 `secret_env` 与加密凭据引用。接入配置包含以下字段。

- `transport`：`webhook` 或 `websocket`，旧配置默认为 Webhook，扫码自动模式保存启用的 WebSocket 配置。
- `intents`：长连接的显式事件订阅，默认 `1 << 25`（私聊与群）。当前连接状态公开实际订阅值。

- `enabled`：默认 true；自动扫码会保存并启用，手动扫码保存为 false。自动模式在服务内完成接收器安装，持久配置用于重启恢复。
- `markdown_enabled` / `rich_messages_enabled`：默认 `true`，QQ 私聊/群及频道图片、Markdown 有对应 renderer。若关闭，显式富消息失败，不静默丢弃按钮或附件。
- `streaming_enabled` / `typing_enabled`：默认 `false`。按应用逐项开启并验收；只在私聊且回复上下文有效时执行。
- `c2c_reply_window_seconds` / `group_reply_window_seconds`：未设置时保留旧窗口。官方页面时限存在冲突，默认仍保守使用 300 秒；C2C 最大4次，群最大5次。
- `official_messages_per_minute`：每会话默认20。
- `official_bot_messages_per_minute`：每应用默认30，可按认证权限降低/提高；官方拒收和429始终优先。
- `official_messages_per_day`：每会话滚动24小时默认1000。按已有 Outbox 尝试记录保守计数，并非腾讯配额实时余额。
- `credential_ref` / `credential_store_dir`：扫码配置使用加密文件引用，主密钥放 `QQBOT_CREDENTIAL_KEY` 环境变量。

旧 `allowed_users` / `allowed_groups` 不是统一权限系统的替代品，不能假设空列表会拒绝访问。正式启用前必须检查 `PermissionPolicy`。

## 管理入口

- `/admin/qq-onboarding`：本地二维码、服务端自动接入、连接状态及手动保存。详见 [扫码说明](qq-onboarding.md)。
- `/api/v1/qq/{instance_id}/messages`：发送富消息到已知会话，先进入持久化 Outbox，不直接调用 QQ。
- `/api/v1/qq/{instance_id}/buttons`：生成有限只读动作按钮，返回键盘结构，不自动发送。
- `/api/v1/qq/{instance_id}/menu` 与 `/panels`：菜单、面板管理，详见 [交互说明](qq_interactions.md)。

以上管理 API 均需要现有管理员 Bearer 认证和统一权限授权。没有启动时自动改菜单或主动发送测试消息。

### 发送 Markdown / 按钮

`request_id` 由调用方产生，用于同目标幂等；更改内容应使用新 ID。同一 ID 重试返回同一投递，不代表已送达。

```json
{
  "conversation_id": "已知会话的内部ID",
  "request_id": "operator-unique-001",
  "text": "# 请选择\n请点击按钮。",
  "content_format": "markdown",
  "reply_to_event_id": "该会话内的有效入站事件ID",
  "official_content": {
    "keyboard": {"id": "已配置的模板键盘ID"}
  }
}
```

`reply_to_event_id` 必须属于目标会话。主动发送须明确 `allow_proactive=true`，且应用 `proactive_verified=true`、目标权限允许；被动窗口过期不会无条件切换主动发送。

### 媒体与卡片

```json
{
  "conversation_id": "已知会话的内部ID",
  "request_id": "operator-unique-002",
  "text": "",
  "reply_to_event_id": "该会话内的有效入站事件ID",
  "official_content": {
    "media": {"type": "image", "url": "https://example.org/image.png"}
  }
}
```

媒体仅接受公开 HTTPS URL，拒绝本地地址、嵌入凭证和非标准端口。QQ 私聊/群由文件上传接口获取 `file_info`，每次目标绑定，`srv_send_msg=false`；上传失败不会接着发送。频道/频道私信图片使用独立的 `image` URL 字段，不调用群/私聊上传接口。本地生成文件另走受限 `qq.file.send` 链路，见[本地文件发送](qq-local-files.md)，不接受任意服务器路径。

`media.type` 为 image/audio/video/file。卡片使用 `official_content.card`，含 title、description、url、pic_url；不能和媒体/模板混在一条消息。`official_content.reference.message_id` 必须是官方引用索引，不能把普通消息 ID 当成引用索引。

## 流式可靠性

Pi 只透传 assistant 公开文本，不透传 thinking、工具参数、工具结果。公开文本可能包含工具调用前的简短进度说明，不只是最终答案。worker 只写已有持久化事件，不持有 QQ 密钥；Core 才能调用 QQ。

有效私聊回复支持普通 Leader 对话及任务进展、提问和最终答复。首段安全文本立即进入投递，后续按16字符合并；已知凭据、Bearer 和潜在协议标记的跨片段尾部会暂存。Core 每轮投递刷新节流缓冲，即使没有新文本事件也会继续发送已缓存的正文。

每条流预留回复序号，先记录 SENDING 再发请求。原生分片维护索引、流 ID、最终状态和累计前缀，不能任意回改已发文字。每次写入重新检查权限及任务有效性。

本地单条流累计正文上限为10000字符。QQ 回执的 `remain_msg_len` 保留作观测值；成功接收分片时可能返回0，因此不据此限制后续正文。

- 收到最终回答事件且 QQ 最终包明确成功后，才将同一最终 Outbox 消息标记为已由流送达。
- 不确定回执或进程中断进入 UNKNOWN，不自动重发完整答案；需人工核对。
- 同一 Leader 输入的旧流尚未收尾或回执未决时，后续 attempt 等待核对；独立的新输入仍可处理。
- 已知安全失败可降级普通最终回复。
- 输入状态独立消耗回复序号；额度不足时不无限重复。
- 未开启、群聊、频道或过期上下文不启动私聊流式。

## 兼容迁移与恢复

运行 `qqbot migrate` 升级至0021，加入无任务 Leader 流和最终投递绑定；既有流式状态及回执保留。原有文本 Outbox JSON 仍可读取；旧纯文本呈现保持，富消息作为原子条目保留，不能按段复制键盘/媒体。历史 OneBot/NapCat 条目按下线规则结算，当前配置仅支持官方适配器。

官方自动 Markdown 对长度不超过10000字符的完整答案保留格式，超长内容明确沿旧纯文本拆分路径发送，避免截断；这不保证 QQ 客户端支持所有 Markdown 方言。

## 上线前必做验收

1. 在沙箱/测试机器人完成 Webhook challenge 和验签，确保公网 HTTPS 回调只接收对应 AppID。
2. 私聊、群聊分别测试所有消息类型，检查手机/桌面渲染和受限内容错误。
3. 验证被动时限、msg_seq、主动拒收、限频、媒体过期；重复点击不会重复执行。
4. 验证流式取消、断线、回执丢失、重启、最终包、thinking不外泄。
5. 验证扫码超时、取消、并发会话、配置冲突、凭证加密和禁用状态。
6. 频道/频道私信需独立验收文本、URL 图片、Markdown 和键盘；不要因 QQ 私聊成功就宣称全部场景通过。
7. 无凭证/权限时只报告代码与模拟测试，不宣称真实绑定或发送成功。

官方参考：
- https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_users_user_openid_messages.post.html
- https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_users_user_openid_stream_messages.post.html
- https://bot.q.qq.com/wiki/develop/api-v2/server-inter/message/overview.html
- https://github.com/tencent-connect/qqbot-agent-sdk

### 频道协议补充（第二轮修复）

已依据腾讯官方 `tencent-connect/botpy` 的 `api.py`（固定提交
`e25f3e84bad7217357d8200a9d12939b58285b84`，`post_message`、`post_dms`）补齐频道和频道私信的 URL 图片、普通/模板 Markdown、键盘与引用编码。频道 payload 不使用 QQ 私聊/群的 `msg_type`、`msg_seq` 或 `/v2/.../files` 上传；图片使用 `image` URL。
仍拒绝频道音频、视频、文件和 QQ 图文卡片；未实现本地文件 multipart。SDK 提及频道在线状态及主动消息限制，实际权限、展示和可用性仍需平台联调，代码支持不代表应用已获得能力。

## 第二轮修复与 Agent 工具

任务中的 Agent 可发现 `qq.message.send`、`qq.message.status`、`qq.buttons.create`。身份、收件会话和任务由 Core 绑定，不能从工具参数指定其他收件人。发送先写入持久化 Outbox；工具返回入队状态不是送达承诺，必须查询状态。`request_id` 在任务内幂等，不同内容复用同一 ID 会被拒绝。文本原子发送上限1800字符，Markdown/富消息上限10000；`qq.message.send` 不接收任意本地路径；当前任务 `outputs/` 文件可使用 `qq.file.send`，见[本地文件发送](qq-local-files.md)。

按钮现在支持有限选择、静态翻页、任务输入及两步取消确认，绑定当前用户和任务。选择只作为普通任务输入，不视为安全/财务等高风险操作授权。详细字段见交互文档。ACK 的总期限为2.5秒，包含获取 token；ACK 失败不执行动作。崩溃于领取动作之后不自动重放，以避免重复副作用。

流式在取消、异常、容量耗尽和缺少最终事件时，只结束已确认的文本前缀，不泄漏未发送缓冲。结束回执不明、撤权或回复超时会保留 UNKNOWN，不能保证远端已关闭，也不会重复发送普通答案。正文首次发送前，输入状态持续60秒、每55秒尝试续期，按当前共享回复序号预算保留至少一个回答额度；已有正文流时优先使用正文。普通消息也在 token 等待和媒体上传后再次检查当前权限与回复期限。

## 关系与主动消息拒收

新增迁移0013持久化已验签的 `FRIEND_ADD/DEL`、`C2C_MSG_RECEIVE/REJECT`、`GROUP_ADD_ROBOT/DEL_ROBOT`、`GROUP_MSG_RECEIVE/REJECT`。好友/群关系移除阻止投递；关闭主动消息仅阻止主动发送，不阻止有效被动回复。重复或较旧事件不能重新开启拒收，同时间戳冲突优先拒收。恢复接收事件不等于授权，仍须 `proactive_verified`、显式主动发送意图和当前统一权限许可。不会自动发送欢迎消息。

事件字段及频道消息结构已对照腾讯官方 SDK 固定提交：
https://github.com/tencent-connect/botpy/tree/e25f3e84bad7217357d8200a9d12939b58285b84
对应 `botpy/manage.py`、`botpy/connection.py`、`botpy/api.py`。平台业务错误码会保留在失败回执，不把所有业务错误当作可重试网络失败。

## 仍待补齐与实测

自动语音转写、视频理解、文件内容提取、任意服务器路径/超大文件上传、通用动态表单/分页、全部频道消息类型、撤回/表情互动/群管理及完整类型化出站 @ 尚未覆盖。递归入站投影是有界语义提取，不代表所有嵌套格式都能完整理解或原样转发。

二维码轮换仅允许已禁用且停止的同 AppID 实例。不同 AppID 要新建实例，避免复用旧权限与历史身份；旧密文保留，不在腾讯侧自动撤销。换绑、远端解绑及一键启用未实现。所有新增能力仍需授权测试应用进行真实 QQ 联调，模拟测试不能证明账户拥有平台权限。
