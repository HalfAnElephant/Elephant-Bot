# QQ 官方 Bot 本地文件发送

本机制针对官方 Bot，未扩展或改动 NapCat 的文件协议。后续未特别指定的平台操作按官方 Bot 能力处理，不应自动换成个人号/NapCat。

## Agent 使用

先在当前任务工作区的 `outputs/` 目录生成文件，再调用：

```json
{"request_id":"report-001","path":"outputs/report.pdf","media_type":"file","text":"报告已生成"}
```

工具名：`qq.file.send`。`media_type` 为 `image`、`audio`、`video` 或 `file`。工具只使用当前任务、当前会话及请求者身份，不接受目标QQ、绝对路径或任意服务器路径。返回 `PENDING` 只是持久化入队，不是已送达；使用 `qq.message.status` 查询 `delivery_id`。`UNKNOWN` 不自动重发。已有 `qq.message.send` 公网URL方式保留。

同一任务的 `request_id` 重试只能对应相同文件和消息，改变内容必须使用新ID。本地文件当前仅发送至官方QQ私聊/群。频道/频道私信尚未核验独立官方大小上限，因此明确拒绝本地上传，不套用私聊/群限额；既有频道公网URL图片方式保留。其他场景是否有文件/音视频权限及支持格式仍以QQ平台实际返回为准。

## 安全与生命周期

- 只读 `outputs/` 下的相对路径；拒绝越界、软链接、硬链接、隐藏/凭证目录和非普通文件。
- 读取、摘要和本地持久化在后台线程执行；读取前、入队前、每次外部上传和最终消息发送前重新检查权限。
- 文件大小改为腾讯官方 `qqbot-nodejs` 固定提交 `429a0f1b798f3b0fcef3679e207dbc0469a1df2d` 的类型常量，包含上限本身：图片30×1024²＝31,457,280字节；语音20×1024²＝20,971,520字节；视频/文件100×1024²＝104,857,600字节。官方文档称MB，源码以1024²计算。
- 工作区快照、Worker→Core及Core→QQ分片均使用有界流式读取，不再用大文件Base64 JSON。每任务400MiB、总暂存1600MiB是累计存储资源配额，不是单文件平台限制，不会阻止单个合法100MiB文件。配额已满时明确拒绝，先清理已过期无引用文件，不覆盖已有投递。
- 文件名、扩展名、内容签名和 MIME 必须相符。支持约束后的PNG/JPEG/GIF/WebP、MP4、SILK/MP3/WAV/OGG、PDF、ZIP、UTF-8 TXT/Markdown/CSV/JSON及无宏OOXML；MIME识别不是病毒扫描，也不保证QQ服务支持每个格式。
- Core将字节暂存到数据库旁私有 `qq-media` 存储，文件0600、目录0700；Outbox只持有不透明引用，不保存文件内容、Base64或原工作区路径。
- 引用绑定任务、会话、请求者与内容摘要。其他任务/用户即使知道引用也不能读取发送。
- 24小时到期；下一次本地发送前清理已过期且无未完成投递的暂存和旧孤立文件。待处理/UNKNOWN引用受保护，不为清理而破坏核对证据。超过有效期的文件不再发送，需新ID重新提交。没有独立定时清理守护任务。
- 取消不能撤回已经开始或完成的外部操作；不会在取消后继续下一分片或发送新消息。未完成远端上传依赖腾讯过期处理，未发现官方远端取消接口。

## 协议说明

QQ私聊/群：`upload_prepare` → 严格校验分片计划 → 独立COS客户端PUT → `upload_part_finish` → `/files`完成 → 单独发送带 `file_info` 的消息。上传计划最多1024片、计划响应最多1MiB，用于防止恶意响应耗尽资源，不是文件大小限额。COS仅接受腾讯云标准桶域名的HTTPS443，禁止重定向，不转发QQ令牌、Cookie或代理认证；未知上传域名明确拒绝。真实平台若返回其他受信域名，须先核验再扩展，不绕过检查。

频道文档虽定义 `file_image` multipart，但未找到能可靠核验的大小上限；目前不冒用C2C/群上限，返回 `LOCAL_MEDIA_LIMIT_UNVERIFIED`。

官方资料存在版本差异：文档仍把 `file_data` 标为“暂未支持”，所以**未使用Base64直传QQ**。Worker→Core内部改为鉴权原始字节流，校验声明长度与实收长度。旧Base64文件入口明确拒绝，避免继续存在另一套阈值或100MiB JSON副本。

核验来源：
- https://github.com/tencent-connect/qqbot-nodejs/blob/429a0f1b798f3b0fcef3679e207dbc0469a1df2d/USAGE.md#103-类型与尺寸上限
- https://github.com/tencent-connect/qqbot-nodejs/blob/429a0f1b798f3b0fcef3679e207dbc0469a1df2d/src/protocol/utils/file-utils.ts
- https://github.com/tencent-connect/qqbot-agent-sdk/tree/6163b5dc979a2f12379b1916805009075008c3c3
- https://github.com/tencent-connect/bot-docs/tree/645787a45937e5d9c4f0f61afefdffde0f38696e
- https://github.com/tencent-connect/botpy/tree/e25f3e84bad7217357d8200a9d12939b58285b84
- https://cloud.tencent.com/document/product/436/6224
- https://cloud.tencent.com/document/product/436/67768

先执行数据库迁移0014。代码与模拟协议测试不等于实际账号权限和客户端验收；本轮未真实上传或发送QQ文件。
