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

`/health/live` 表示进程响应。`/health/ready` 检查 DB、初始化、NapCat 连接、Core 主任务及关键调度和投递循环。数据库暂时不可用、关键循环连续三次失败、退出或单次执行超过 120 秒时返回 503，恢复成功后自动恢复就绪。首个循环错误即可在 `background.degraded` 查看。

`background.loops` 分别提供 execution、delivery、memory、expression、speakup 的 heartbeat、last_success、last_error、last_error_at、failures、stalled 和 exited。execution 表示 Core 调度的状态，Worker 执行与供应商调用仍通过任务事件核对。可选维护故障按循环与会话隔离，并以最多 30 秒的指数退避重试。维护与主动搭话的单次执行超过 600 秒会标记 stalled。卡住的外部操作保留现场，结合任务、平台回执与 UNKNOWN 核对后处理。

鉴权 `/api/v1/status` 同时显示 Inbox、Outbox 积压计数及最旧待办时间，可供外部监控计算积压年龄。健康接口只查询本地状态。最近实际模型调用结果以任务事件和验收记录为准。

- `QUEUED` 没有启动 runner；取消可立即终结。
- `RUNNING` 有独立 run、lease 与 fencing。租约失效保留槽，不能直接再启动同任务。
- `NEEDS_RECONCILIATION` 必须先用固定 launcher 停止旧 run，再凭 root stop marker 核对。管理 API `POST /api/v1/tasks/{public_id}/reconcile` 接受 `run_id` 和可选的经验证 `TaskResult`；不传结果会关闭为失败/取消/过期。只有核对后的新 ownership 能补交旧 spool，不允许旧 fencing 直接覆盖状态。
- `POST /api/v1/tasks/{id}/retry` 使用新的 `Idempotency-Key` 创建关联新任务；不会覆盖旧任务或复用来源事件。
- 任务完成与发送完成分开。`UNKNOWN` 表示平台可能已接受，不自动重发。核实平台或人工回执后，管理员可 `POST /api/v1/deliveries/{id}/resolve`，提供 `state: SENT|FAILED`、`evidence` 和可选 `platform_message_id`；该操作不发送消息。
- `WAITING_FOR_TRIGGER` 保留任务结果，等待同会话的新有效触发更新官方回复凭据。已成功分段不重复发送。

`/任务`、`/取消` 与其他确定性命令不等待前台模型；`/静默 30m` 支持 s/m/h，最长 24 小时，持久恢复；不带时长则持续静默直到 `/恢复`。`/补充 任务号 内容` 将输入交给原任务。群聊自动回答等待问题时，发起人需引用问题或 at 机器人，其他成员可明确引用问题参与。外部条件等待中的补充会与真实观察满足后的继续输入一起处理。

产物接受有界文本、Office、PDF、图片、CSV、JSON 与 ZIP，具体扩展名由 `DEFAULT_ARTIFACT_MIME_TYPES` 限定。导出后有 UUID、大小和 SHA-256。鉴权下载读取并校验同一内容快照，拒绝符号链接、硬链接与变化内容；任务创建 30 天后管理下载返回 410。NapCat 的文件与图片单独进入 Outbox，待发送引用在 24 小时后过期，发送前重验归档摘要、来源与授权。平台回执不确定时保持 UNKNOWN，全部附件完成后通知才记为 SENT。官方 QQ 接入会说明附件未发送，并记录附件失败。

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

社交模式要求真实分组隔离的标注回放；`evals/social_replay.py` 消费显式标注数据。`evals/fixtures/social-baseline.jsonl` 是 16 条合成策略回归，预置 signals 仅检查确定性策略，不衡量真实模型选择质量。Jev、embedding 与自动提取需要各自的供应商凭据，缺失时明确跳过/降级。仅有协议夹具通过不能开放 social。


## 调用计量与配置诊断

鉴权 `GET /api/v1/model-calls?limit=100` 合并 Core 的判别、表达、记忆、embedding、主动搭话请求与 Worker 的任务、压缩及失败说明请求。Core 记录耗时、排队时间与关联身份，Worker 沿现有任务及 run 记录。缺失 usage 的计数保持 null，coverage 为 unknown。费用及定价版本在缺少价格输入时保持 null。该接口返回最近的有界样本，不能直接当作全历史账单。

工具完成事件带 capability、耗时、成功或异常类型及最近成功读取的技能路径与固定版本。技能字段表示本轮加载上下文，组合多个技能时仍需结合具体工具轨迹核对归属。日志保留允许列出的关联 ID 和异常栈帧，不保存模型正文或凭据。

配置页的 diagnostics 显示保存配置的 expected_model、Core 的 core_loaded_model、expected_skills，以及 Worker 的模型指纹、可用技能版本和在途已加载版本。Worker 每 30 秒经内部认证 API 上报，超过 90 秒标记 stale，未上报显示 unknown。源技能版本与已发布版本不一致时先发布技能快照，在途任务保持原版本。模型失配列出 Core 或 Worker 待重启组件，保存设置不会替换正在运行的进程。

## 历史与自动记忆

`history-memory` 技能通过宿主查询原文和已发布记忆。查询和读取均重新检查当前会话、权限、来源撤回与墓碑，返回稳定 source ID。中文搜索使用子串候选与 n-gram 匹配，返回扫描游标供继续翻页。查询 embedding 限时 1.5 秒，失败退回词法检索，缓存按主体、会话、索引版本和模型版本隔离。

`memory.auto_extract` 启用候选提取，`memory.auto_publish_self_reports` 默认允许有明确自述原文、同主体证据与高置信低风险标记的事实发布。推断保留 candidate，冲突保留 disputed。管理页可查看证据并发布普通候选，维护拒绝原因通过 `GET /api/v1/memory-maintenance?conversation_id=...` 查看。黑话候选要求真实出现、至少三个上下文和两名说话人，发布仍沿既有词典审查策略。

任务恢复从已有目标、输入、检查点派生状态，按真实接入顺序排列用户修订。丢失或撤回的来源会阻止恢复使用，超过请求字节门会明确失败，原始档案与输入仍保留。历史指代可使用摘录摘要的来源索引恢复原文，再沿正常上下文的范围与来源检查。业务进展由任务状态恢复。
