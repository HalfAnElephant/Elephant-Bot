# 邮箱与收信任务

邮箱复用现有 Leader、Worker、任务权限与 `schedules` 持久化。账号、规则、来信回执和发送记录由原用户、身份、会话及项目持有。

## 账号与凭据

通过自然语言指挥 Leader 调用 `mail.accounts` 创建、查询、修改、测试、停用或删除账号。普通配置包含账号标识、邮箱地址、登录名和 IMAP/SMTP 参数，支持 iCloud、QQ、163 预设及自定义 TLS/STARTTLS 服务端。

模型只接触普通参数、凭据引用与状态。密码、授权码或应用专用密码通过账号回执的 `setup_path` 提交。实际页面为 `/api/v1/mail?account_id=<账号 UUID>`，页面壳允许直接打开，账号 API 与凭据提交均要求管理员 bearer 令牌。令牌仅保存在页面内存中。手动配置也使用该页面，可选择已验证的用户对话及其固定项目创建账号，编辑时保留原作用域。远程访问使用 HTTPS 或可信本地连接。

- iCloud 使用启用双重认证后创建的应用专用密码。
- QQ、163 需要在服务商设置中启用 IMAP/SMTP，并使用服务商生成的客户端授权码。
- `ready` 表示账号已启用且具有凭据引用，`tested` 表示当前修订已通过 IMAP 和 SMTP 登录测试。测试过程只认证，不发送邮件。
- 修改连接参数后旧凭据失效，需通过受保护入口重新提交。IMAP 身份变更会递增 `mailbox_generation`，防止不同邮箱相同 UID 被混用。停用、删除及所有者授权撤销会阻止后续访问。

加密凭据保存在数据库目录旁的私有 `mail-credentials/` 下，沿用 `ELEPHANT_CREDENTIAL_KEY`。备份与恢复须同时保留数据库、加密凭据文件及受保护保存的主密钥。日志和任务产物不得记录凭据明文。

## 读取、附件与发送

`mail.folders` 列出文件夹。`mail.messages.list/search/get` 支持按 UID 分页、关键词搜索及指定邮件读取，返回解码后的中文主题和文本、线程字段及附件元数据。邮件内容标记为不可信外部输入。IMAP 使用只读选择和 `BODY.PEEK`，正文编码体总量上限 256 KiB，截断时 `body_complete=false`。附件通过 `mail.attachment.save` 按需进入现有工作目录与文件能力，读取时核对 UIDVALIDITY，附件编码体上限 20 MiB。

`mail.send.prepare` 固定收件人、正文、回复线程、附件和内容摘要。`mail.send` 使用返回的草稿编号、账号 UUID 及摘要执行，重复请求沿用同一持久化回执。普通后台与时间触发的发信任务通过任务或 schedules 的 `requested_actions` 记录用户明确授权的 `mail.send`；收信触发同时遵守原始与当前 `mail_permissions`。

SMTP 的 `ACCEPTED`、`PARTIAL_ACCEPTED` 分别表示服务器接受全部或部分收件人。最终投递仍需独立证据。`FAILED` 保留实际失败状态，`UNKNOWN` 与进程在发送中断开的情况保留待核对记录，状态查询使用 `mail.send.status`，核对结果后再决定后续动作。连接成功和发送任务去重不能替代外部送达证据。

确认失败的草稿可由 `mail.send.retry` 复验摘要、账号和当前授权后恢复为待发送，保留原内容及失败摘要，再单独调用 `mail.send`。结果不明的发送通过 `/api/v1/mail/outgoing-ui` 由管理员提交核对证据，确认未发送后可恢复重试；同一任务与账号存在未核对结果时暂停新增发送。

## 收信规则

- `mail` 保存账号、文件夹及 `all`（全部）、`any`（至少一个）、`none`（全部排除）筛选组。发件地址/域名、主题与正文支持精确或包含判断，筛选由程序完成。
- `goal`、`success_checks`、`requested_outputs` 保存用户任务目标、完成条件与输出文件。命中后，Agent 通过 `mail.input` 读取宿主冻结的触发邮件，复用普通任务执行与回报链。
- 已配置且凭据就绪的账号支持 `enabled=true`，尚待配置时可以保存禁用草稿。启用和重新启用默认从当前 UID 水位开始，用户明确要求历史回放时才设置 `mail_replay_from_uid`。账号或文件夹变化、时间触发与邮件触发互换需新建规则。
- `mail_permissions` 使用现有 `PermissionRule` 记录任务获准执行的动作及资源。宿主按触发时记录与当前规则的交集复验，并同时遵守当前账号、项目及全局权限。模型、任务编排与原会话回报由宿主管理，业务能力按用户目标授权。
- 本地文件成果可授权 `workspace.file.*`，资源使用 `workspace:*` 与准确的 `file:outputs/文件名` 或获授权前缀。发信另需 `mail.send` 到 `mail-account:<账号 UUID>` 及每个 `mail-recipient:<地址>`。`mail-recipient:$sender` 只展开为触发邮件的原始 From，Reply-To 不会扩大该授权。
- 后台增量轮询逐封保存来信和规则回执，任务提交、游标推进与去重回执使用同一事务。SEARCH 上界固定在本轮 SELECT 水位，新到来邮件留待下一轮。UIDVALIDITY、账号代际、规则修订与启用代际分别阻止旧引用重用。
- 修改、暂停或删除规则沿用 `schedules.get/list/update/cancel` 及当前 `revision`。邮件正文和附件不能改变规则或授予新权限。

## 验证

确定性回归覆盖账号作用域、加密凭据、TLS 协议顺序、MIME/中文、分页与 UID 边界、逐封触发、撤权、事务回滚、发送内容冻结和模糊结果恢复。`evals/dialogue_live.py` 使用真实 DeepSeek 和 Pi，通过签名 QQ 入站验证自然语言创建和修改普通账号配置、缺少凭据时的真实状态、未知账号追问，以及中文条件禁用草稿的 SQLite 持久化。

在线闭环另通过受保护 HTTP API 提交合成凭据，由可信测试宿主注入邮箱协议观测。真实模型通过中文请求创建两个启用规则，收到合成摘要邮件与工单邮件后，实际 Worker 读取 `mail.input`、调用 `workspace.write` 生成文件，Leader 验收归档并向本地 QQ 接收器发送最终回报。报告以 `synthetic_mailbox` 标明邮箱观测来源，核对文件哈希、原文事实、任务成功及回执去重。模型参数、答复和文件内容均由实际模型生成。

真实邮箱服务器兼容性及实际收发须分别使用 iCloud、QQ、163 和自定义账号验收，测试结果按服务商单独记录。真实 QQ 客户端回报也需单独验收。
