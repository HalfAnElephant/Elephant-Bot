# 运维与恢复

## 分享首次启动诊断

以下命令适用于提供 `elephant` CLI 和 `--share-safe` 的版本。已发布的 v0.1.0 可用 `qqbot doctor` 本地排查，但不支持脱敏选项，不要分享其原始输出。

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

下列管理 API 都需要 Bearer 管理员认证及对应的对象权限。

## OpenViking 记忆

- `GET /api/v1/memory/status`：查看当前 owner 私人范围的同步状态，需要 `memory.read`。
- `GET /api/v1/memory/changes?after={seq}`：读取后续记忆变更，需要 `memory.read`。
- `POST /api/v1/memory/reconcile`：只读核对恢复后的远端状态及删除屏障，固定使用当前 owner 的私人范围，需要 `memory.write`。

## 任务操作核对

- `GET /api/v1/tasks/{public_id}/operations`：列出任务副作用记录，需要 `task.operations.read`。
- `GET /api/v1/tasks/{public_id}/operations/{operation_id}/readback`：按当前凭据和权限回读副作用，需要 `task.operations.read`。
- `POST /api/v1/tasks/{public_id}/runs/{run_id}/reconcile`：核对已停止的 Worker 运行。请求体必须且只能选择 `{"resume": true}` 或 `{"result": {...}}`，需要 `task.reconcile`。未能验证停止证据时，Core 会拒绝结算或恢复。


## 备份和隔离恢复演练

先核对任务和外部操作，按原部署方式停止 daemon，并确认没有仍在改变数据的 Worker 或持久 Job。保留原服务单元与 drop-in。SQLite 快照和删除账本是两件事。备份快照不会复制配置、密钥、Pi 会话文件、工作区、邮箱凭据目录或外部 OpenViking 服务。备份已停止实例的配置及有效运行数据目录，保留原始权限；凭据和加密密钥按私密资料分开保存，不与可分享诊断、发布附件或任务文件混放。恢复 QQ/邮箱加密凭据要用原有效密钥，不能重生成代替。外部服务的备份和实际删除状态由对应服务核对。

在**已安装的完整发布目录**（含 `alembic.ini` 和 `migrations/`）中运行。用原服务身份、启动配置和环境运行。当前开发版用 `.venv/bin/elephant`；运行已发布的 v0.1.0 时改用 `.venv/bin/qqbot`。变量使用绝对路径，备份目录须由服务身份可写且权限为 `0700`，目标文件必须尚不存在；不要用生产管理员令牌或外部服务配置启动演练实例。

```sh
umask 077
CLI=.venv/bin/elephant  # v0.1.0: .venv/bin/qqbot
"$CLI" backup --config "$INSTANCE_CONFIG" --destination "$BACKUP_DIR/core.snapshot.db"
"$CLI" export-deletions --config "$INSTANCE_CONFIG" --destination "$BACKUP_DIR/deletions.json"
"$CLI" restore --config "$INSTANCE_CONFIG" --snapshot "$BACKUP_DIR/core.snapshot.db" \
  --deletion-ledger "$CURRENT_DELETION_LEDGER" --destination "$RESTORE_DB"
```

- `CURRENT_DELETION_LEDGER` 在演练中可指向刚导出的账本；恢复旧快照时必须来自**当前有效的实例**，不能拿快照当日的旧账本代替。无法取得可信账本则不要重新开放记忆或外部任务。
- `RESTORE_DB` 必须不存在。恢复会使用执行命令的 CLI 所对应的 schema；先核对回退目标的兼容性。检查恢复文件的完整性，用隔离配置指向它，禁止复用生产身份、Webhook、邮件发送和任务入口。检查预期的任务与结果可读，删除记录没有复活；只有合成数据齐全的隔离实例才执行合成首任务。
- 恢复会把未确认投递和正在运行的任务留在需核对状态。先按上面的回读入口检查实际结果，不把未知当失败或自动重试，不用历史投递作为新送达证据。
- OpenViking 走[恢复后核对](../openviking.md#数据流与边界)的删除屏障；不能证明远端任务、来源、URI 安全时不开放召回。最终恢复还需人工核对已经发生的外部动作、快照之后的任务丢失窗口与配置/密钥权限。
