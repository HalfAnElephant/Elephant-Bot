# 运维与恢复

## 分享首次启动诊断

```sh
uv run elephant doctor --share-safe --config configs/local.yaml > doctor-report.json
```

该命令读取启动 YAML 与现有运行时配置覆盖，在临时 SQLite 中检查迁移、完整性与 FTS5，对正式数据库不做迁移或写入。它不启动 daemon、不连接远端，也不会解密已保存的 QQ 凭据。普通 `doctor` 保留路径和警告，适合自己排查；要分享时用上面的 JSON，不要附原始配置或环境文件。

- `checks.sqlite`：本机临时数据库与迁移的检查结果，不代表正式数据库或服务已经就绪。
- `checks.dependencies`：只显示 Node、Pi、FFmpeg 能否找到，不声称版本、Pi 启动或执行隔离已通过。
- 模型、QQ 和 OpenViking 分开显示 `not_configured`、`incomplete` 或 `configured`；个人 OpenViking 可显示 `disabled`。`configured` 只反映本地字段、凭据存在或存储引用，不代表凭据有效、官方 QQ 已送达、OpenViking 账户可访问或 Worker 隔离可用。它们仍标为 `not_tested`。
- `effective` 只显示本次读取的权限预设/默认效果、Leader/Worker 执行环境、CLI 内置监听。不是部署进程或反向代理的实况，也没有改变这些值。

配置可读取且临时数据库检查通过时退出码为 `0`，即使没有配置可选服务。临时数据库检查失败为 `1`；配置无法读取或校验为 `2`。失败也只输出安全状态；本地详细排错可改用普通 `doctor`。

部署、服务账户、配置和启动说明见 [Ubuntu 部署说明](../../deploy/README.md)。后台 Job 的生命周期、回执与主机验收见 [独立监督的后台 Job](../operations/persistent-jobs.md)。

## 升级前备份与回退演练

先在管理页核对运行中的任务和外部操作，然后按原部署方式停止 daemon，并确认没有正在改变数据的 Worker 或持久 Job。保留原启动 YAML、私有环境文件（尤其是现有 `ELEPHANT_CREDENTIAL_KEY`）、服务单元及 drop-in，以及应用数据目录中的运行配置、会话、工作区、操作证据和邮件凭据。不要用新示例替换旧配置、重生成密钥或调整服务权限来做常规升级。

从对应版本的应用根目录，用原服务身份、原启动配置和原环境执行。将 `CONFIG` 指向实际启动 YAML，`BACKUP_DIR` 指向原服务身份可写、权限为 `0700` 的私有目录。以下目标文件必须尚不存在：

```sh
umask 077
.venv/bin/elephant backup --config "$CONFIG" --destination "$BACKUP_DIR/pre-upgrade.db"
.venv/bin/elephant export-deletions --config "$CONFIG" --destination "$BACKUP_DIR/deletions.json"
```

迁移失败或需要回退时，保留当前数据库和日志。在可读时从**最新**数据库再次导出删除账本，使用最新有效账本，避免恢复已经删除的内容。先在不连接真实模型、QQ、邮箱或 OpenViking 的环境中恢复到**新文件**，不覆盖在线数据库：

```sh
.venv/bin/elephant restore --config "$CONFIG" --snapshot "$BACKUP_DIR/pre-upgrade.db" \
  --deletion-ledger "$LATEST_LEDGER" --destination "$BACKUP_DIR/staged-restore.db"
```

恢复不等于从头重发：未确认的外部投递仍需回读，未结算的任务仍需核对停止证据。包含 OpenViking 删除记录时，恢复会把新文件迁移到执行恢复命令的 CLI 所用的 schema；先用目标代码和保留的配置核对兼容性，不要假定旧二进制能打开它。先验收备份副本、就绪与任务记录，再决定是否切换；正式恢复后的远端状态通过下述接口分别核对。

下列管理 API 都需要 Bearer 管理员认证及对应的对象权限。

## OpenViking 记忆

- `GET /api/v1/memory/status`：查看当前 owner 私人范围的同步状态，需要 `memory.read`。
- `GET /api/v1/memory/changes?after={seq}`：读取后续记忆变更，需要 `memory.read`。
- `POST /api/v1/memory/reconcile`：只读核对恢复后的远端状态及删除屏障，固定使用当前 owner 的私人范围，需要 `memory.write`。

## 任务操作核对

- `GET /api/v1/tasks/{public_id}/operations`：列出任务副作用记录，需要 `task.operations.read`。
- `GET /api/v1/tasks/{public_id}/operations/{operation_id}/readback`：按当前凭据和权限回读副作用，需要 `task.operations.read`。
- `POST /api/v1/tasks/{public_id}/runs/{run_id}/reconcile`：核对已停止的 Worker 运行。请求体必须且只能选择 `{"resume": true}` 或 `{"result": {...}}`，需要 `task.reconcile`。未能验证停止证据时，Core 会拒绝结算或恢复。
