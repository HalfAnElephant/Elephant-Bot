# Elephant Bot

QQ 官方机器人上的个人助手。一个 Python daemon 管理持久 Leader 对话、根目标和按需 Worker；Pi 管理各 Agent 的原生历史与压缩，OpenViking 管理长期记忆，Hermes 原生工具管理标准 `SKILL.md`。

首次使用从[本地启动](#本地启动)开始，各平台的[安装和后台运行方式](deploy/README.md)见部署说明。完整运行目录包括 Python 包、迁移文件和 npm 依赖。

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

个人安装支持 Linux 和 macOS；Python 由 uv 按锁定版本准备，Pi 使用 Node.js 22.19.0 或更新版本。完整的系统隔离及持久 Job 需要 Linux、systemd、cgroup v2 和 bubblewrap。NapCat/OneBot 接入、社交 Judge、主动插话、表达学习、旧人格覆盖系统、research 专用执行器与独立 Worker API 已退役。

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

表中代码路径相对 `src/elephant/`。SQLite 保存恢复依据，Pi Session 保存原生历史，OpenViking 保存长期记忆，systemd user manager 监督持久 Job。daemon 重启后的处理以停止证明和原始回执为依据。

## 本地启动

推荐使用已校验的 [GitHub Releases](https://github.com/HalfAnElephant/Elephant-Bot/releases/latest) 完整发布包；也可在可信的源码检出目录安装。解压后保留完整目录。在目录内运行：

```sh
./install.sh --apply --setup
```

Linux 个人安装不需要 sudo，先准备 [uv](https://docs.astral.sh/uv/getting-started/installation/)、Node.js 22.19.0 或以上（含 npm）和 FFmpeg 6 或以上。macOS 用户有 Homebrew 时，可加 `--with-deps` 让安装器补齐依赖，不会自动安装 Homebrew。只想预览时运行 `./install.sh`；安装后运行 `./elephant` 可重新打开向导。

个人版升级时，先备份、排空任务并停止旧进程，再把新发布包解压到一个新目录安装。安装器不会覆盖已有运行环境；默认配置和数据不随发布目录改变。

安装时优先复用与项目锁定版本一致的系统 Pi；如果没有，也没有可用的 Elephant 私有副本，安装器会先询问。默认不安装，不覆盖系统 Pi。无人值守安装时，可明确加上 `--install-pi` 允许在需要时安装私有副本。

Linux 常驻服务支持 Ubuntu、Debian、Fedora 和官方 Arch Linux；macOS 可选择当前窗口运行，或登录当前用户时运行。版本、架构、限制与升级流程见[各平台部署](deploy/README.md)。不要把新安装当作旧数据迁移。

向导分四步：选择模型服务和推荐模型、按需检测连接、用手机 QQ 扫码、选择是否直接启动。普通终端用方向键和回车操作，窄终端或不支持彩色菜单的终端可用编号。常用服务不需要填写接口地址；仅自定义服务需要提供地址与模型名称。密钥输入不回显。

内置 DeepSeek、OpenAI、OpenRouter、通义千问（阿里云北京）、Kimi（国际站）、GLM（Z.AI 国际站）、Google Gemini、Groq 和 xAI 的 API 配置，不包含订阅登录或 OAuth。可选检测只验证模型能否回复，插件工具、多轮对话和地域可用性另行验证。重复运行默认保留已有设置和 QQ 绑定。可以单独更换当前 API 密钥，无需重选服务和模型；更换服务时则需要新服务的密钥。如果本地 Elephant 正在运行，先停再改模型或密钥，或在管理页调整；系统服务沿用管理页明确保存的模型。仅完成绑定或网关接通不代表消息已送达，用 QQ 发一条私聊确认首个回复。

### 手动配置（可选）

自动部署不需要逐项编辑配置。手动配置或排错可从示例开始：

```sh
uv sync --locked
npm ci --ignore-scripts
cp configs/example.yaml configs/local.yaml
cp .env.example .env
chmod 600 .env configs/local.yaml
```

编辑 `configs/local.yaml` 的 `model`。例如使用本项目真实 API 回归覆盖的 DeepSeek 配置。

```yaml
model:
  endpoint: https://api.deepseek.com/chat/completions
  model: deepseek-flash
  api_key_env: ELEPHANT_MODEL_KEY
  context_window: 32768
```

其他供应商填写其完整 Chat Completions 地址、模型名和实际支持的上下文窗口。`api_key_env` 是环境变量名。编辑 `.env`，将供应商密钥填入 `ELEPHANT_MODEL_KEY`，将下面两条命令的输出分别填入 `ELEPHANT_ADMIN_TOKEN` 和 `ELEPHANT_CREDENTIAL_KEY`。

```sh
uv run python -c 'import secrets; print(secrets.token_urlsafe(32))'
uv run python -c 'import base64,secrets; print(base64.b64encode(secrets.token_bytes(32)).decode())'
```

管理员令牌用于登录管理页。凭据加密密钥用于 QQ 扫码接入，生成一次后随私有配置备份，后续重启和升级沿用原值。实际密钥通过进程环境提供，管理页保存环境变量名。`ELEPHANT_OFFICIAL_SECRET` 供手动配置已有 QQ 应用使用，自动扫码流程会加密保存应用凭据。

```sh
set -a
. ./.env
set +a
uv run elephant validate --config configs/local.yaml
uv run elephant doctor --config configs/local.yaml
uv run elephant serve --config configs/local.yaml
```

`serve` 迁移数据库并监听 `127.0.0.1:8080`。示例已启用管理任务，默认适配器列表为空。`validate` 检查配置结构并提示缺失的首次配置；`doctor` 检查临时数据库、迁移、FTS5、可执行文件位置和配置告警。先补齐相关告警，再提交首次任务。真实模型和 QQ 连接状态通过后续实际操作确认。

需要分享诊断结果时，运行 `uv run elephant doctor --share-safe --config configs/local.yaml`。它只输出脱敏状态，不输出凭据、服务地址、账户标识或文件路径，不联网，也不修改正式数据库。`configured` 只表示本地配置条件符合，`not_tested` 表示尚未验证联通或隔离。普通 `doctor` 的输出保持不变。详见[脱敏诊断说明](docs/runbooks/operations.md#分享首次启动诊断)。

- [管理页](http://127.0.0.1:8080/admin)
- [存活检查](http://127.0.0.1:8080/health/live)
- [就绪检查](http://127.0.0.1:8080/health/ready)

### 第一个任务

打开管理页并输入 `ELEPHANT_ADMIN_TOKEN`，也可在已加载 `.env` 的另一个终端提交一次任务。

```sh
curl --fail-with-body http://127.0.0.1:8080/api/v1/tasks \
  -H "Authorization: Bearer $ELEPHANT_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: first-hello-1' \
  -d '{"goal":"请用一句中文介绍你能做什么，无需调用工具。"}'
```

使用返回的 `public_id` 查询结果，或在管理页任务列表查看。相同需求重试沿用原 `Idempotency-Key`，新需求使用新值。

```sh
curl --fail-with-body http://127.0.0.1:8080/api/v1/tasks/PUBLIC_ID \
  -H "Authorization: Bearer $ELEPHANT_ADMIN_TOKEN"
```

任务结果为 `SUCCEEDED` 且包含实际模型答复后，首次任务完成。默认 Leader 使用 host，Worker 使用 sandbox。受隔离的命令、凭据 GitHub CLI 与持久 Job 依赖 Linux 对应能力。macOS 和缺少这些组件的 Linux 不会因此切换到不隔离的宿主执行。

### 接入 QQ 与长期记忆

打开[扫码接入页](http://127.0.0.1:8080/admin/qq-onboarding)，输入管理员令牌并创建二维码，使用手机 QQ 授权。流程会绑定扫码者为 owner、保存权限并连接官方长连接。页面显示连接就绪后，发送一条私聊并检查回复。平台账号权限与失败处理见[扫码说明](docs/operations/qq-onboarding.md)。

长期记忆可在首次任务成功后配置。按 [OpenViking 安装与身份绑定](docs/openviking.md) 部署后端、创建普通 USER，并填写 `memory.openviking.bindings` 及对应密钥。示例中的空 endpoint 表示关闭远端记忆。

## 身份、配置和任务

官方 QQ owner 绑定使用 AppID 与验证过的 OpenID。旧数字 QQ 号不能直接作为官方身份。`owner.preferences` 保存明确设置的称呼、语言和沟通偏好。

Leader 的聊天、进度、提问和最终汇报统一加载 `src/elephant/runtime/persona.md`，改编自 Aurabot 大肥鱼，保留“大象”称呼。日常回复简短口语化，长结果按需使用 Markdown。QQ 只展示目标、实际进展和结果，内部编号、队列与执行分工保留在管理和调试接口中。

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

按 [Linux 系统部署说明](deploy/README.md#guided-linux-installation) 安装单 daemon、root 停止证明 helper、Job user manager 和 OpenViking。现有部署升级需先停止旧服务，保留数据库中的输入、操作回执、未知投递和删除屏障。普通启动默认保留迁移前备份；明确要求不备份的部署可调用 `migrate(path, backup=False)`。

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
