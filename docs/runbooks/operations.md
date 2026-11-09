# 运维与恢复

## 进程和访问

Core 是唯一业务数据库写入者。正式 CLI 在迁移与监听前取得数据库旁 `.core.lock` 独占锁；退出释放，第二个 Core 拒绝启动。崩溃重启只清理由同 UID 持有且连接被拒绝的旧 Unix socket，不删除普通文件、符号链接或仍活动的 socket。

Core HTTP 仅监听 `127.0.0.1:8080`。管理页 `/admin` 的令牌只存在当前页面内存；管理请求需要 Bearer 认证和对象范围检查。经 SSH 转发访问，例如 `ssh -L 8080:127.0.0.1:8080 user@your-server`。官方 Webhook 还需独立反向代理、公开 HTTPS 和平台订阅，不能把管理面一并暴露公网。

Core 以 `qqbot` UID、`qqbot-worker` 主组、`qqbot` 补充组运行。`/var/lib/qqbot` 默认 0700，Worker 仅获 traverse ACL；`/run/qqbot` 0750，内部 socket 0660。Worker 使用独立 32+ 字符令牌访问 UDS。产物目录由 Worker 写入、通过默认 ACL 允许 Core 读取，其余 Core 数据不开放。

`/etc/qqbot/core.env`、`worker.env`、`worker.token` 分别保存所需凭据，不写入源码。主供应商密钥不传给 runner；runner 只拿本次模型与工具能力 token。配置 `tasks_enabled=true` 之前先完成目标主机隔离检查。

## 部署目录与 runner 网络

正式执行快照使用 root 控制的 `/opt/qqbot`，Core 配置位于 `/etc/qqbot/core.yaml`，服务模板见 `deploy/`。本地配置、环境变量文件与 `var/` 运行数据保持在 Git 忽略范围内。更新前先核对任务状态，再安排服务重启。

NapCat 作为独立服务运行，可通过配置的正向 WebSocket 连接。连接方式、端口和白名单按实际环境设置。

runner 使用专用 network namespace，无默认路由，只能访问 `10.203.0.1:30000..30999`。目标主机使用 UFW 时，按实际策略配置该内部链路的入口。

```sh
sudo ufw allow in on qqbot-host proto tcp from 10.203.0.2 to 10.203.0.1 port 30000:30999 comment 'Elephant isolated runner capability proxy'
```

`qqbot-runner-network.service` 在 Worker 之前创建 namespace。目标主机应完成隔离、取消、恢复及开机启动验收，再启用服务。安装细节见 [Ubuntu 部署说明](../../deploy/README.md)。

## 健康、任务与投递

`/health/live` 仅表示进程响应；`/health/ready` 还检查 DB、初始化和 NapCat 连接。管理 `/api/v1/status` 的模型字段表示已配置，不表示真实供应商成功。最近实际调用结果以任务事件和验收记录为准。

- `QUEUED` 没有启动 runner；取消可立即终结。
- `RUNNING` 有独立 run、lease 与 fencing。租约失效保留槽，不能直接再启动同任务。
- `NEEDS_RECONCILIATION` 必须先用固定 launcher 停止旧 run，再凭 root stop marker 核对。管理 API `POST /api/v1/tasks/{public_id}/reconcile` 接受 `run_id` 和可选的经验证 `TaskResult`；不传结果会关闭为失败/取消/过期。只有核对后的新 ownership 能补交旧 spool，不允许旧 fencing 直接覆盖状态。
- `POST /api/v1/tasks/{id}/retry` 使用新的 `Idempotency-Key` 创建关联新任务；不会覆盖旧任务或复用来源事件。
- 任务完成与发送完成分开。`UNKNOWN` 表示平台可能已接受，不自动重发。核实平台或人工回执后，管理员可 `POST /api/v1/deliveries/{id}/resolve`，提供 `state: SENT|FAILED`、`evidence` 和可选 `platform_message_id`；该操作不发送消息。
- `WAITING_FOR_TRIGGER` 保留任务结果，等待同会话的新有效触发更新官方回复凭据。已成功分段不重复发送。

`/任务`、`/取消` 与其他确定性命令不等待前台模型；`/静默 30m` 支持 s/m/h，最长 24 小时，持久恢复；不带时长则持续静默直到 `/恢复`。当前 harness 不支持 `/补充`，明确拒绝，不假称已排队。

产物仅接受有界 UTF-8 Markdown/text/JSON，导出后有 UUID、大小和 SHA-256。鉴权下载读取并校验同一内容快照，拒绝符号链接、硬链接与变化内容；任务创建 30 天后下载返回 410。

## 备份、迁移与删除

```sh
uv run qqbot backup --config configs/local.yaml --destination /safe/new-backup.sqlite
uv run qqbot export-deletions --config configs/local.yaml --destination /safe/current-deletions.json
uv run qqbot restore --config configs/local.yaml --snapshot /safe/new-backup.sqlite --deletion-ledger /safe/current-deletions.json --destination /safe/restored.sqlite
```

SQLite backup API 可取得一致在线快照。升级前先保存快照；停 Core/Worker 后迁移，失败不启动新代码，用新路径恢复旧快照并重放最新删除台账，检查 integrity 后再切配置。不要原地覆盖活动 WAL 数据库。迁移命令只用于停服维护，不支持两个迁移者同时运行。

恢复将旧 SENDING 标为 UNKNOWN、活动任务置为待核对；已删除来源不能被旧记忆或摘要重新发布。逻辑字段清空不是对 WAL、磁盘扇区或外部旧备份的物理擦除。备份也包含用户数据，应限制访问和保留时间。

## 配置和评估

管理配置预校验后，以 expected_version 发布。模型和发送预算可热更新；连接、身份、judge、隔离等变更要求重启。版本列表保留发布前配置，读取旧版本为候选后使用当前 expected_version 应用，完成显式回滚。

社交模式要求真实分组隔离的标注回放；`evals/social_replay.py` 不生成虚假标注数据。Jev、embedding 与自动提取需要各自的供应商凭据，缺失时明确跳过/降级。仅有协议夹具通过不能开放 social。
