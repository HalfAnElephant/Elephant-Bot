# QQ official QR onboarding

The administrator can open `/admin/qq-onboarding`. This flow configures an **official bot application**, not a personal QQ account. A scanner is not automatically an Elephant administrator.

## Deployment preparation

Set `QQBOT_ADMIN_TOKEN` as usual. Provision `QQBOT_CREDENTIAL_KEY` in the Core process secret environment as a base64-encoded, randomly generated **32-byte** encryption key. Keep it outside runtime configuration, logs, and database backups. The page refuses to create a binding task when this key is missing or malformed. Back up this key through the deployment's secure secret-management process; losing it makes saved credentials unreadable. Rotation is not yet automatic.

Use HTTPS or a private local tunnel for management access. The UI uses the same administrator bearer authentication and permission policy as the existing console. It does not persist bearer tokens in browser storage. Sessions are tied to the authenticated token and are memory-only; restarting Core invalidates unconfirmed sessions.

## Flow

1. Enter administrator token and a new adapter instance name.
2. Create a binding session. QR rendering happens locally on the server, not through an external QR image service.
3. Scan using mobile QQ and approve on Tencent's page.
4. Verify the AppID displayed in Elephant, then explicitly choose **confirm and save**.
5. The server validates the received credentials against Tencent's token endpoint, writes an encrypted app-bound credential file, and saves a configuration reference. No existing adapter or AppID is automatically overwritten.
6. The new adapter is saved with `enabled=false`. Configure its Webhook and unified `PermissionPolicy` for allowed subjects and conversations, explicitly enable it, then restart Core and perform separate live acceptance. Legacy `allowed_users`/`allowed_groups` are not an access-control boundary. No full-observation permission, proactive permission, or administrator role is granted by the QR flow.

Credential files use AES-256-GCM with AppID as authenticated additional data, private directory permissions (0700), and private file permissions (0600). Neither the application secret nor the temporary AES key is returned to the browser. The scanner OpenID is not retained. Configuration snapshots contain only a reference and private-store path.

## Lifecycle and limits

The implemented polling interval is two seconds and the local confirmation deadline is five minutes, following SDK defaults rather than claiming service hard limits. Platform expiration is distinct from local timeout. There are at most eight active sessions and 128 retained session records. Each adapter instance can have only one active session. Cancel and expired sessions discard pending secrets; cancelling after a successful save does not revoke Tencent authorization or delete configured credentials.

Cancellation during token validation prevents persistence. Closed pages stop browser polling; server sessions expire independently. Cancellation of a network request may not undo Tencent-side activity. Restarting the service cancels local jobs and clears temporary materials.

Credential validation does not establish working Webhook, permissions, client rendering, or message delivery. This implementation has mock protocol/security tests; real QQ onboarding must be accepted with the application owner present.

Protocol reference: https://github.com/tencent-connect/qqbot-agent-sdk/blob/main/src/qqbot_agent_sdk/onboard.py (MIT). The implementation follows the documented protocol; it does not vendor that SDK.

## 同应用凭证轮换

创建扫码会话时可传 `replace_app_id`（页面有对应字段），必须精确匹配既有官方实例的 AppID。先在配置禁用实例并重启，运行中的实例拒绝轮换。扫码得到的 AppID 必须相同，保存前再次检查配置版本、当前权限和停止状态。保存只替换凭证引用，保留其余调优/策略，并保持禁用；旧加密文件保留供管理员核对，未执行远端凭证撤销。不同应用身份应创建新实例，不允许复用旧身份的权限范围。成功保存后仍需显式启用并重启。
