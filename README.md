# Elephant Bot

QQ 官方机器人上的个人助手。一个 Python daemon 管理持久 Leader 对话、根目标和按需 Worker；Pi 管理各 Agent 的原生历史与压缩，OpenViking 管理长期记忆，Hermes 原生工具管理标准 `SKILL.md`。

- 同一 Dialogue 可以挂多个目标；Leader 派发后结束轮次，Worker 独立推进。
- 任务、输入、操作回执和投递状态保存在 SQLite。等待会让出执行资格，取消与恢复需要真实停止证明。
- 后台 Worker 按需执行，取消固定名额与总执行时长限制，模型请求保留上游并发背压。Linux 上的持久 Job 由 systemd user manager 监督，可跨 daemon 重启继续运行。
- QQ 保留四种官方 surface、媒体、按钮、Markdown、私聊流式及权限检查。实际能力取决于应用权限与平台验收。
- 长期记忆按验证过的 owner、工作区和可见范围绑定。更正与忘记使用 epoch 和删除屏障；后端故障时聊天继续，不建立备用事实库。
- Skills 使用锁定版本的 Hermes 工具、标准文件和条件复盘，Leader 与 Worker 共用各自 owner 的目录。
- 定时安排持久保存触发时间或 cron、时区与原始身份，触发后提交普通任务树。重启默认只补最近一次，修改使用版本检查。
- QQ 图片直接交给模型的多模态输入。文件原件下载到当前工作文件夹，Agent 通过正常文件和执行工具读取、转换或处理。
- 使用官方 DeepSeek 模型地址及已配置的模型密钥时，联网工具调用 DeepSeek Search／Fetch。`deepseek-flash` 还可按需调用 `document.parse`，以文字或图文方式解析工作区文档，并将文件引用交给当前对话。上传回执按用户、会话和项目隔离，供重试及重启复用，远端文件设置 30 天有效期。
- QQ 语音优先使用平台转写，否则通过 DeepSeek ASR 自动转写为本轮口述请求，转写随 Pi 输入持久保存。支持 SILK 及 FFmpeg 可解码的常见音频，单条上限 8 MiB、120 秒。历史和转发语音保留资料来源，识别失败时要求重发或改用文字。

需要 Python 3.13、uv、Node.js >= 22.19.0、npm 和 FFmpeg >= 6。协议开发支持 macOS/Linux；生产使用 Linux、systemd、cgroup v2 和 bubblewrap。NapCat/OneBot 接入、社交 Judge、主动插话、表达学习、旧人格覆盖系统、research 专用执行器与独立 Worker API 已退役。

## 架构与代码入口

QQ 官方 Webhook/WebSocket 和受保护的管理入口将输入写入 Inbox。Core 处理身份、权限与持久 Dialogue，Scheduler 调度 Leader、Worker 和技能复盘。Leader 直接回答或提交根目标，Worker 独立执行，结果与问题回到 Leader，经 Outbox 持久投递。

| 子系统 | 代码入口与职责 |
| --- | --- |
| 接入与管理 | `app.py`、`adapters/qq_official/`、`admin/` 负责 HTTP、官方 QQ 连接和扫码绑定 |
| 对话与调度 | `runtime/core.py`、`sessions.py`、`scheduler.py`、`leader.py` 管理持久对话与 Agent 轮次 |
| 任务与恢复 | `runtime/task_graph.py`、`recovery.py`、`jobs/service.py` 管理目标树、修订、租约和停止后恢复 |
| 模型与执行 | `harness/`、`providers/`、`adapters/pi/` 连接 Pi 原生 Session、模型背压和宿主工具网关 |
| 后台工作 | `adapters/execution/`、`capabilities/` 管理受控工作区、持久 systemd Job 与外部操作回执 |
| 记忆与技能 | `adapters/openviking/`、`adapters/hermes_skills/` 管理长期记忆和标准技能文件 |
| 持久化与投递 | `storage/`、`ingress/`、`outbox/` 保存输入、任务、操作证据与消息投递状态 |

表中代码路径相对 `src/qqbot/`。SQLite 保存恢复依据，Pi Session 保存原生历史，OpenViking 保存长期记忆，systemd user manager 监督持久 Job。daemon 重启后的处理以停止证明和原始回执为依据。

## 本地启动

在仓库根目录运行：

```sh
uv sync --locked
npm ci --ignore-scripts
cp configs/example.yaml configs/local.yaml
cp .env.example .env
chmod 600 .env configs/local.yaml
```

配置模型的完整 Chat Completions 地址、模型名及 `api_key_env`。通过进程环境提供实际密钥；管理页只保存环境变量名。至少配置 `QQBOT_ADMIN_TOKEN` 和模型使用的密钥变量。官方 QQ 另需实际 AppID/AppSecret；长期记忆使用 OpenViking 对应普通用户的凭据。

```sh
set -a
. ./.env
set +a
uv run qqbot validate --config configs/local.yaml
uv run qqbot doctor --config configs/local.yaml
uv run qqbot serve --config configs/local.yaml
```

`serve` 迁移数据库并监听 `127.0.0.1:8080`。默认适配器列表为空，可先使用受保护的管理任务入口；它创建真实 host 任务，不伪造 QQ 消息。`doctor` 检查临时数据库、迁移与 FTS5，不代表模型、QQ 或生产环境已经验收。

- [管理页](http://127.0.0.1:8080/admin)
- [存活检查](http://127.0.0.1:8080/health/live)
- [就绪检查](http://127.0.0.1:8080/health/ready)

## 身份、配置和任务

官方 QQ owner 绑定使用 AppID 与验证过的 OpenID。旧数字 QQ 号不能直接作为官方身份。`owner.preferences` 保存明确设置的称呼、语言和沟通偏好。

Leader 的聊天、进度、提问和最终汇报统一加载 `src/qqbot/runtime/persona.md`，改编自 Aurabot 大肥鱼，保留“大象”称呼。日常回复简短口语化，长结果按需使用 Markdown。QQ 只展示目标、实际进展和结果，内部编号、队列与执行分工保留在管理和调试接口中。

完整配置见 [示例](configs/example.yaml) 与 [Schema](configs/schema.json)。管理页支持校验、版本、保存和有限热应用；连接、身份、模型及运行边界的修改需要重启 daemon。网页配置写入启动数据库旁的 `runtime-config.json`，历史版本在 `config-versions/`。保存配置不会自动搬迁数据库或部署服务。

管理 API 使用管理员令牌，实时权限策略使用既有 PermissionService。任务详情和附件受真实持久身份、owner 和当前权限约束；管理任务可使用 `POST /api/v1/tasks`，必须提供稳定的 `Idempotency-Key`，同键不同目标会被拒绝。

| QQ 命令 | 用途 |
| --- | --- |
| `/帮助`、`/状态` | 命令与确定性服务状态 |
| `/任务 [编号]`、`/取消 <编号>` | 查询或取消任务树 |
| `/补充 <编号> <内容>` | 追加输入或回答持久化问题 |
| `/新对话 <名称>`、`/切换对话 <名称或编号>`、`/对话` | 管理持久 Dialogue |
| `/项目 [项目名]` | 列出或选择已配置项目，`personal` 返回个人范围 |
| `/静默 [30m]`、`/恢复` | 当前会话静默与恢复 |

自然语言交给 Leader 判断直接答复或委派。任务完成、成果验证和 QQ 送达是独立状态；排队不表示已经启动，UNKNOWN 外部操作必须先核对。

项目名来自 `orchestration.workspace_bindings` 或当前 owner 的 OpenViking workspace 绑定。每个 Dialogue 固定项目，切换项目使用独立 Session；旧任务和定时安排继续使用原项目。项目记忆使用独立 OpenViking USER，技能保存于该 owner 的 `projects/<项目名>/skills/`。群访问项目需要明确的当前群权限规则，并启用相应的群记忆或技能设置。

定时请求可直接描述执行目标、时间或周期和时区，Leader 使用 `schedules.create/list/get/update/cancel` 管理。管理接口位于 `/api/v1/schedules`，创建要求 `Idempotency-Key`，修改和删除要求当前 `expected_revision`。取消安排停止未来触发，已经派发的工作使用任务控制入口管理。

技能管理接口提供 `/api/v1/skills`、`/read`、`/history`、`/export` 和 `/rollback`，均使用管理员令牌与当前权限。回退需要目标 entry ID 和该技能的最新 ledger ID，历史变化时拒绝覆盖。`workspace_id` 指定已配置的项目或 `personal`。导出 ZIP 包含标准技能文件与附件。

已有事项的补充进入原 Worker；相同原文、附件和上下文的活动请求复用已有工作。不同原文的需求由 Leader 结合当前目标判断。长工作委派后，Leader 简短确认并继续处理聊天，进度和结果统一经 Leader 表达，相同进度合并，新修订使待发旧进度失效。

Worker 超时、阻塞或一次尝试无法继续时会保存已有结果并通知 Leader。Leader 可安排关联重试、在相关执行停止且外部操作已核对后接手，或保存问题等待用户回答。旧 `max_active`、并发、排队和任务树数量字段只作配置兼容，后台 `active_seconds`／`total_wall_seconds` 不再终止工作。单次工具、等待条件和供应商请求仍有各自的超时语义；异常交给 Leader 决策。各 Task 与独立 Leader 输入轮次分别保留模型/工具调用预算，同一 Task 的 continuation 与 attempt 共用累计用量；Leader 输入 retry 也沿用原预算。根目标只累计记录全树 `model_calls`／`tool_calls`，不另行汇总活跃时间或总墙钟时间。

## 部署和恢复

按 [Ubuntu 部署说明](deploy/README.md) 安装单 daemon、root 停止证明 helper、Job user manager 和 OpenViking。现有部署升级需先停止旧服务，保留数据库中的输入、操作回执、未知投递和删除屏障。普通启动默认保留迁移前备份；明确要求不备份的部署可调用 `migrate(path, backup=False)`。

- [实现与完整验收矩阵](docs/elephant-agent-implementation-plan.md)
- [持久 Job](docs/operations/persistent-jobs.md)
- [QQ 官方接入](docs/operations/qq_official.md)
- [技能文件与原生工具](docs/learned-skills.md)
- [运维入口](docs/runbooks/operations.md)
- [CI 和发布](docs/runbooks/releases.md)

备份恢复必须应用最新删除记录，再核对远端记忆状态。旧 Pi 历史不能在遗忘后重新作为当前上下文打开；未知副作用不能因重启或通知失败被重新执行。恢复工具和真实主机验收的边界见部署文档。

## 验证与隐私

GitHub Actions 执行回归、控制台检查、lint、schema 校验、构建和安装冒烟。仓库测试含真实锁定 Pi 配合确定性模型夹具；真实供应商调用、QQ 客户端和 Linux 隔离分别验收。

```sh
uv run pytest -q
node --test tests/console_ui.test.cjs
uv run ruff check src tests evals scripts
```

凭据、有效配置、数据库、原生 Session 和产物应放在仓库外并限制权限；提交前检查实际文件。管理员令牌仅保存在页面内存。远程管理使用 SSH 转发或受保护的 HTTPS 入口，勿公开内部能力网关。

原创代码采用 AGPL-3.0-only。网络部署应提供对应源码入口（`source_url`）。第三方代码与锁定版本见 [第三方说明](THIRD_PARTY_NOTICES.md) 和 [版本锁定](upstreams.lock.yaml)。
