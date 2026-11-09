# Elephant Bot

独立的 QQ AI 机器人，支持 OneBot v11 / NapCat 与 QQ 官方 Webhook，提供 pi 对话执行、会话记忆、持久化通用任务和 Web 管理页。

## 功能与运行环境

- 双接入共用 Inbox / Outbox，消息、任务与投递状态持久化到 SQLite。
- 普通对话与后台任务共用 pi Agent Loop，通过能力发现调用网页、GitHub、工作区和受控命令。
- 支持会话内记忆的保存、纠正与遗忘，可选接入候选提取和 embedding 服务。
- 独立 Worker 提供前后台交付、warm session、进度通知、等待续接、取消与恢复核对，文件按用户目标生成。
- 统一 PermissionService 默认 allow_all，可按主体、操作和资源配置规则。
- 管理页提供会话、发言策略、任务、记忆和配置管理。

需要 Python 3.13、uv、Node.js >= 22.19.0 和 npm。本地开发支持 macOS / Linux，生产隔离 runner 需要支持 systemd、网络命名空间与 cgroup 的 Linux 主机。凭据隔离的 GitHub CLI 和工作区命令还需要 bubblewrap。所有命令在仓库根目录执行。

## 快速开始

### 1. 安装依赖

```sh
git clone https://github.com/HalfAnElephant/Elephant-Bot.git
cd Elephant-Bot
uv sync --locked
npm ci --ignore-scripts
cp configs/example.yaml configs/local.yaml
cp .env.example .env
chmod 600 .env configs/local.yaml
```

### 2. 设置凭据

编辑 `.env`，在需要的变量后填写实际值。管理令牌和内部令牌分别用下面的命令生成，填入 `QQBOT_ADMIN_TOKEN` 与 `QQBOT_INTERNAL_TOKEN`。

```sh
uv run python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

| 环境变量 | 用途 | 使用条件 |
| --- | --- | --- |
| `QQBOT_ADMIN_TOKEN` | 管理页与管理 API 认证 | 使用管理功能 |
| `QQBOT_MODEL_KEY` | 兼容配置字段；实际执行使用 Worker 专用密钥 | 按部署配置选择 |
| `QQBOT_ONEBOT_TOKEN` | NapCat / OneBot Access Token | 启用 OneBot 接入 |
| `QQBOT_OFFICIAL_SECRET` | QQ 官方机器人 AppSecret | 启用官方接入 |
| `QQBOT_INTERNAL_TOKEN` | Core 与 Worker 的内部令牌，至少 32 字符 | 启用普通对话或后台执行 |
| `QQBOT_RUNNER_OPENAI_KEY` | Worker 模型代理的独立供应商密钥 | 普通对话与后台任务的实际模型请求 |
| `QQBOT_GITHUB_TOKEN` | 可选 Worker 专用 GitHub 令牌 | 启用凭据隔离的 GitHub/Git 能力 |
| `QQBOT_JUDGE_KEY` | 可选发言判断服务密钥 | 配置 `jev` / `structured` judge |
| `QQBOT_MEMORY_KEY` | 可选记忆候选提取服务密钥 | 启用自动提取 |
| `QQBOT_EMBEDDING_KEY` | 可选 embedding 服务密钥 | 启用向量索引 |

程序从进程环境读取密钥。每次启动前，在同一个终端显式加载自己填写的 `.env`。

```sh
set -a
. ./.env
set +a
```

`.env` 使用 shell 赋值语法，包含特殊字符的值应使用单引号包裹。systemd 部署通过 `EnvironmentFile` 注入对应进程所需的变量。

### 3. 检查并启动

```sh
uv run qqbot validate --config configs/local.yaml
uv run qqbot doctor --config configs/local.yaml
uv run qqbot serve --config configs/local.yaml
```

默认配置启动一个适配器列表为空、交互执行开启的 Core，需要先设置内部令牌。`serve` 自动迁移数据库并监听 `127.0.0.1:8080`。`doctor` 检查临时数据库、迁移和 FTS5，QQ 连接与模型调用需要继续按下面的步骤验收。

打开管理页，输入 `QQBOT_ADMIN_TOKEN` 的值后读取状态。

- [本地管理页](http://127.0.0.1:8080/admin)
- [存活检查](http://127.0.0.1:8080/health/live)
- [就绪检查](http://127.0.0.1:8080/health/ready)

### 网页控制台

`/admin` 提供全部 Core 配置的分类表单（含 QQ 接入、模型、执行预算、上下文、
发言判断、记忆、路径与环境变量名），也可切换到高级 JSON 编辑。支持校验、
历史版本载入、保存下次启动配置，以及允许字段的即时应用。全局人格和实时权限策略
有独立编辑入口，保存后立即生效；原有会话、任务、记忆管理继续可用。

连接后进入配置中心，可按八个分类浏览或搜索中文名称、字段路径。修改会显示实际变更
数量，改回原值自动清除草稿状态；保存前展示原值与新值，依据配置选择即时应用或等待
重启。字段校验错误会定位到对应输入项，版本冲突、网络失败与令牌失效保留当前草稿。
“撤销修改”恢复上次读取的内容，退出或重新读取前会提示处理未保存修改。

- **即时应用**：发送预算；关闭交互和后台执行时也可应用模型元信息。
- **保存并等待重启**：其他 Core 设置。页面分别显示运行配置与已保存配置，保存不会
  重启服务或中断任务。模型变更还需同步 Worker 的 `--model`、`--model-endpoint`
  及部署许可名单，再重启 Core / Worker。
- **并发与恢复**：保存使用版本校验，冲突时需重新读取；历史版本先载入候选，再校验、
  保存或应用。磁盘写入失败不会修改运行配置。
- **凭据与主机设置**：控制台只编辑环境变量名、显示 Core 进程内是否已配置，不读取或
  保存密钥值。密钥仍通过 `.env` / systemd `EnvironmentFile` 注入；Worker 启动参数、
  root runner policy、网络和系统服务配置仍由部署端管理。

网页保存文件为启动 YAML 中 `database` 所在目录的 `runtime-config.json`，历史记录在
同目录 `config-versions/`；保存后该文件覆盖启动 YAML，CLI 的验证、备份和启动等命令
均读取有效配置。即使在网页修改数据库路径，配置文件仍留在原目录；这不会搬迁旧数据库，
迁移数据应使用备份/恢复流程。要恢复直接使用 YAML，可在停机备份后移走该覆盖文件。

全局人格保存在当前数据库目录的 `persona-global.json`，不改写随代码提供的人格文件；
更换 `persona_path` 后不再使用旧路径的人格覆盖。会话人格覆盖仍单独保存。
权限页编辑数据库中的实时策略；Core 配置中的 `permissions` 仅作为尚未建立数据库策略时
的初始值。控制台拒绝会令管理员失去权限管理入口的整份策略。

令牌仅保存在当前页面内存，断开或刷新后需重新输入。远程访问可使用 SSH 端口转发，或通过 HTTPS Tunnel 单独发布管理入口；
所有管理 API 均需要管理员令牌。Tunnel 只匹配 `/admin`、静态资源和 `/api/v1/`，
不要把 QQ Webhook、OneBot WebSocket 或内部服务一起公开。

## 配置模型与 QQ 接入

配置文件为 `configs/local.yaml`。下面各段合并到该文件，接入标识、账号、群号与模型名称按实际环境替换。完整字段和默认值见配置 schema。

### 模型与执行 Worker

Core 配置记录模型、窗口和配置版本。Worker 的 `--model`、`--model-endpoint` 与 `QQBOT_RUNNER_OPENAI_KEY` 控制实际 pi 模型请求，两端按部署模型对齐。`endpoint` 使用完整的 Chat Completions 地址。

```yaml
model:
  endpoint: https://api.example.com/v1/chat/completions
  model: your-model-id
  api_key_env: QQBOT_MODEL_KEY
  timeout_seconds: 30
```

普通问题和自然语言状态说明需要可用 Worker。签名校验、Inbox 与管理接口独立运行。默认人格位于 `personas/elephant_default.yaml`，可通过 `persona_path` 指向自己的文件。

先在另一个终端启动本地协议开发 Worker，并加载同一份环境变量。创建受限内部令牌文件后，指定实际模型与地址。

```sh
mkdir -p var
printf '%s' "$QQBOT_INTERNAL_TOKEN" > var/worker.token
chmod 600 var/worker.token
uv run python -m qqbot.jobs.worker \
  --socket var/internal.sock --token-file var/worker.token \
  --pi "$(pwd)/node_modules/.bin/pi" --workspace var/runners \
  --spool var/worker-spool --archive var/artifacts --runner-mode local \
  --model your-model-id --model-endpoint https://api.example.com/v1/chat/completions
```

macOS 本地模式用于 pi 协议开发。凭据隔离的 GitHub 操作与工作区子进程在目标 Linux 上验收。生产安装步骤见后文。

### OneBot v11 / NapCat

先独立安装并登录 NapCat，启用其 OneBot WebSocket 服务，并设置 Access Token。以下配置让 Core 连接 NapCat 的正向 WebSocket。

```yaml
adapters:
  - instance_id: napcat
    kind: onebot
    bot_id: 'YOUR_BOT_QQ'
    secret_env: QQBOT_ONEBOT_TOKEN
    websocket_url: ws://127.0.0.1:3001
```

`QQBOT_ONEBOT_TOKEN` 与 NapCat 配置的 Access Token 保持一致。账号和群号使用字符串。应用权限默认全部放行，可通过统一权限规则限制群、个人或群内成员。旧 `allowed_users`、`allowed_groups`、`admin_users` 字段保留兼容数据和角色元信息，实际操作由当前 PermissionService 决定。

如使用反向 WebSocket，省略 `websocket_url`，将 NapCat 连接地址设为 `ws://127.0.0.1:8080/onebot/napcat`，并发送相同的 Bearer Access Token。URL 最后一段必须与 `instance_id` 一致。

保存配置并重启 Core，在已接入私聊发送 `/状态`，再发送一句普通问题检查模型回复。管理页可查看适配器连接状态。

### QQ 官方 Webhook

创建并配置官方机器人应用，将配置加入同一个 `adapters` 列表。

```yaml
  - instance_id: official
    kind: official
    bot_id: YOUR_BOT_ID
    app_id: YOUR_APP_ID
    secret_env: QQBOT_OFFICIAL_SECRET
    full_mode_enabled: false
    full_mode_verified: false
    proactive_verified: false
```

在平台配置公开 HTTPS 回调地址 `https://YOUR_DOMAIN/webhooks/qq/official`，通过反向代理转发到 Core。仅公开所需回调路径，管理访问使用本机或 SSH 转发。权限规则中的主体 ID 使用事件里的平台 OpenID。

全量群消息与主动投递需要实际应用权限及目标环境验收。任务结果受平台回复窗口约束，必要时等待同会话的新触发后投递。

### 其他常用字段

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `deployment_id` | `elephant-local` | 部署身份，参与持久化身份计算，已有数据的部署应保持稳定 |
| `database` | `var/core.db` | SQLite 路径，运行账号需要写权限 |
| `source_url` | 空字符串 | 对应部署版本的源码地址，发布网络服务时填写 |
| `send_budget_per_minute` | `0` | 每分钟发送上限；`0` 不限，兼容 1–60 的显式限额 |
| `timezone` | `Asia/Shanghai` | 时区 |
| `interactive_execution_enabled` | `true` | 普通对话提交至 pi Worker |
| `tasks_enabled` | `false` | 启用显式 `/研究` 后台提交 |
| `permissions.preset` | `allow_all` | 默认无额外操作限制 |
| `execution.foreground_wait_ms` | `5000` | 等待预算到期只改变交付方式 |
| `execution.max_active` / `execution.sustained_limit` | `4` / `2` | Core 总执行与持续任务容量 |
| `internal_socket` | `var/internal.sock` | Core 内部 Unix socket |
| `judge.kind` | `rule` | 可选 `rule`、`jev`、`structured` |
| `memory.maintenance_enabled` | `false` | 增量摘要、保留期清理与索引维护 |
| `memory.auto_extract` | `false` | 模型提取记忆候选，需配置提取 endpoint、model 与密钥 |

完整配置与限制见 [示例配置](configs/example.yaml)、[JSON Schema](configs/schema.json) 和 [发言与记忆说明](docs/social-implementation.md)。默认保持 `addressed_only`，启用 social 模式前需要真实标注回放评估。

## 常用 QQ 命令

| 命令 | 作用 |
| --- | --- |
| `/帮助`、`/状态` | 查看命令与运行状态 |
| `/记住 <内容>`、`/我的记忆` | 保存和查询本会话个人记忆 |
| `/纠正 <记忆ID> <内容>`、`/忘记 <记忆ID>` | 纠正或撤销记忆 |
| `/静默 30m`、`/恢复` | 通过当前权限策略设置临时静默或恢复发言 |
| `/研究 <目标>` | 提交通用后台目标，需开启 tasks_enabled 并运行 Worker |
| `/任务 [编号]`、`/取消 <编号>` | 查询任务或请求取消 |
| `/补充 <编号> <内容>` | 补充活跃目标或回答持久化问题 |

## 通用执行与 Linux 部署

普通对话和后台任务都需要 Core 与可信 Worker。生产使用隔离 runner。启用实际执行前完成以下配套配置。

1. 按部署说明安装 `/opt/qqbot` 运行环境、独立服务账号、固定 root launcher 与网络命名空间。
2. 在 Core 配置 `tasks_enabled: true`、`internal_socket: /run/qqbot/internal.sock`，设置独立内部令牌。
3. 将相同内部令牌写入受限的 `/etc/qqbot/worker.token`，为 Worker 配置独立的 `QQBOT_RUNNER_OPENAI_KEY`。
4. 对齐 Worker 的 `--model`、完整 `--model-endpoint` 与 root policy 中的模型许可名单。仓库服务模板中的供应商与模型值需要按部署环境调整。
5. 完成目标主机的 `pi-isolation-check --enable-policy` 检查，配置产物目录权限，再启动 Core / Worker / 网络服务。

Core 模型元信息与 Worker 实际模型配置保持一致。生产 runner 使用 `--runner-mode systemd`，本地协议开发可使用 `local` 模式。

- [Ubuntu 安装与隔离检查](deploy/README.md)
- [任务控制与恢复契约](docs/task-implementation.md)
- [Harness 工具与产物边界](docs/harness-contract.md)
- [A01–A62 验收证据与限制](docs/agent-execution-acceptance.md)
- [运维、备份与投递核对](docs/runbooks/operations.md)

## 开发与验证

```sh
uv run pytest -q
node --test tests/console_ui.test.cjs
uv run ruff check src tests evals scripts
```

测试使用合成消息和本地模型夹具。部分测试会运行 `node_modules/.bin/pi`，请先安装 npm 锁定依赖。真实 QQ 收发、供应商调用和目标 Linux 主机隔离分别验收。

## 文件与隐私

凭据通过环境变量注入，本地配置放在 `configs/local.yaml`，数据库、会话记忆、人格覆盖和任务产物放在 `var/`。这些位置及常见密钥、备份、日志文件已加入 `.gitignore`。现场验收记录和截图保留在本地。

自定义目录或导入用户档案时，将实际配置、档案清单和运行数据放到仓库外，并检查 `git status --short` 后再提交。备份含用户数据，应限制权限并设定保留期。

## 许可证

原创代码采用 AGPL-3.0-only。提供网络服务时，应向使用者提供对应版本源码入口，可通过 `source_url` 配置。第三方依赖、参考项目与许可证据见 [第三方说明](THIRD_PARTY_NOTICES.md) 和 [版本锁定清单](upstreams.lock.yaml)。
