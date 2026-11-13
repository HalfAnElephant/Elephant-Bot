# QQ 官方扫码接入

管理入口为 `/admin/qq-onboarding`。页面默认自动接入，管理员创建二维码后，扫码者在手机 QQ 确认授权，服务端继续完成凭据校验、加密保存、个人 owner 绑定、权限配置和官方 WebSocket 连接。关闭页面后，服务端继续处理已接受的流程。

## 一次性部署准备

在 daemon 的环境文件配置 `QQBOT_ADMIN_TOKEN` 与 `QQBOT_CREDENTIAL_KEY`。后者为随机生成的 32 字节密钥的 Base64 编码，用于 AES-256-GCM 加密应用凭据。home 使用 `/etc/qqbot/core.env`，密钥在后续扫码和服务重启时持续复用。密钥应由部署秘密管理流程保管，已有加密凭据依赖原密钥。

管理页面使用现有 Bearer 认证和统一权限策略。自动模式另核验配置保存与权限管理权限；服务端在远程校验返回后再次检查当前管理员身份与权限。管理访问使用 HTTPS 或私有本地隧道。

## 自动流程

1. 输入管理员令牌与实例名称，保留“自动接入，并将扫码账号绑定为个人 owner”，创建二维码。
2. 用手机 QQ 扫码并确认腾讯页面中的应用授权。
3. 服务校验绑定响应中的 AppID、加密凭据及扫码者 OpenID，再验证应用访问凭据。
4. 应用凭据写入私有加密目录，配置保存 `credential_ref` 与路径；`owner.qq_bindings` 保存已核验的 AppID、OpenID 和时间。
5. 统一权限策略为该实例建立 owner 个人允许规则和其他主体拒绝规则，保留其他实例与管理入口的行为。同身份重新扫码保留已有规则和后续人工限制。
6. 保存启用的 `transport=websocket` 配置，停止旧接收器后安装新接收器并刷新运行时身份。收到 READY 或 RESUMED 后页面显示连接就绪。

新实例默认订阅 `GROUP_AND_C2C_EVENT`，即私聊及群事件。其他事件使用 adapter 的显式 `intents` 设置并受腾讯实际授权约束。`INTERACTION_CREATE` 使用 `1 << 26`，频道事件按配置选择，`1 << 9` 同时要求 full mode。连接状态包含实际订阅值及是否订阅按钮回调。

应用身份和 owner 身份均须匹配，已有同 AppID 的不同 owner 绑定会拒绝自动覆盖。同实例重新扫码自动轮换凭据并启用官方长连接，保留原消息限额、intents 和其他调优。旧凭据引用可供已有配置版本核对。

## 状态、重启与失败恢复

`creating → waiting → validating → connecting → connected` 表示自动接入进展。配置已保存和网关已连接分别展示。应用配置、owner 绑定、权限策略和加密凭据在连接前持久化，进程重启后自动读取启用配置并重连。

连接丢失后按有界退避重连，支持心跳 ACK 检查与 Session Resume。权限或分片等不可重试错误显示固定诊断码，readiness 返回失败。状态读取接口为 `GET /api/v1/qq/onboarding/status`，可在服务重启后查看持久配置和当前网关状态。`/health/ready` 同时列出连接与断开的实例。

扫码会话绑定创建者的认证令牌，并在内存中保存临时交换密钥。未保存会话的本地期限为五分钟，官方过期独立处理。取消会清理临时资料。保存后连接生命周期由持久配置管理，扫码会话的取消接口拒绝撤销已保存配置。配置或激活失败保留安全诊断码及审计记录，平台账号权限仍以实际返回为准。

凭据文件目录权限为 0700，文件权限为 0600，AppID 参与加密认证。公开会话状态只返回应用身份、阶段与连接状态，扫码者 OpenID 和应用 Secret 留在服务端。自动绑定在管理员显式选择自动模式后执行。

## 管理 API 与手动配置

创建自动会话使用现有管理员 Bearer 认证。

```json
{"instance_id":"qq-official","automatic":true}
```

轮询 `GET /api/v1/qq/onboarding/sessions/{session_id}`，二维码为同路径的 `/qr`。自动流程在服务端保存并激活。创建接口的 `automatic` 默认值为 false，既有 API 调用继续采用手动保存流程。

手动模式在扫码后显示确认保存，应用保存为禁用配置。Webhook 模式的回调地址为 `/webhooks/qq/{instance_id}`，需要配置公网 HTTPS、平台事件订阅及统一权限后启用。每个实例使用其 `transport` 选择的接收方式。

协议依据为腾讯的 [扫码 SDK](https://github.com/tencent-connect/qqbot-agent-sdk/blob/main/src/qqbot_agent_sdk/onboard.py) 和 [官方事件协议](https://bot.q.qq.com/wiki/develop/api-v2/dev-prepare/interface-framework/event-emit.html)。代码包含协议和故障测试，真实账号授权与收发结果在部署验收中另行记录。
