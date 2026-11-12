# 运维与恢复

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
