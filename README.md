# Elephant Bot

独立的 QQ AI 机器人，支持 OneBot v11 / NapCat 与 QQ 官方 Webhook，提供文本对话、会话记忆、持久化研究任务和 Web 管理页。

## 功能与运行环境

- 双接入共用 Inbox / Outbox，消息、任务与投递状态持久化到 SQLite。
- 前台对话使用显式配置的 Chat Completions 兼容模型，支持中文人格、会话白名单和发言预算。
- 支持会话内记忆的保存、纠正与遗忘，可选接入候选提取和 embedding 服务。
- 后台研究使用独立 Worker 和 pi runner，提供公开网页检索、报告产物、取消和恢复核对。
- 管理页提供会话、发言策略、任务、记忆和配置管理。

需要 Python 3.13、uv、Node.js >= 22.19.0 和 npm。本地开发支持 macOS / Linux，生产隔离 runner 需要支持 systemd、网络命名空间与 cgroup 的 Linux 主机。所有命令在仓库根目录执行。

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

编辑 `.env`，在需要的变量后填写实际值。管理令牌可用下面的命令生成，再填入 `QQBOT_ADMIN_TOKEN`。

```sh
uv run python -c 'import secrets; print(secrets.token_urlsafe(32))'
```

| 环境变量 | 用途 | 使用条件 |
| --- | --- | --- |
| `QQBOT_ADMIN_TOKEN` | 管理页与管理 API 认证 | 使用管理功能 |
| `QQBOT_MODEL_KEY` | 前台对话模型密钥 | 使用 AI 对话 |
| `QQBOT_ONEBOT_TOKEN` | NapCat / OneBot Access Token | 启用 OneBot 接入 |
| `QQBOT_OFFICIAL_SECRET` | QQ 官方机器人 AppSecret | 启用官方接入 |
| `QQBOT_INTERNAL_TOKEN` | Core 与 Worker 的内部令牌，至少 32 字符 | 启用后台任务 |
| `QQBOT_RUNNER_OPENAI_KEY` | Worker 模型代理的独立供应商密钥 | 运行后台研究 Worker |
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

默认配置启动一个适配器列表为空的 Core，便于先检查安装。`serve` 自动迁移数据库并监听 `127.0.0.1:8080`。`doctor` 检查临时数据库、迁移和 FTS5，QQ 连接与模型调用需要继续按下面的步骤验收。

打开管理页，输入 `QQBOT_ADMIN_TOKEN` 的值后读取状态。

- [本地管理页](http://127.0.0.1:8080/admin)
- [存活检查](http://127.0.0.1:8080/health/live)
- [就绪检查](http://127.0.0.1:8080/health/ready)

## 配置模型与 QQ 接入

配置文件为 `configs/local.yaml`。下面各段合并到该文件，接入标识、账号、群号与模型名称按实际环境替换。完整字段和默认值见配置 schema。

### 前台对话模型

`endpoint` 填写完整的 Chat Completions 请求地址，`model` 填写供应商支持的模型 ID。

```yaml
model:
  endpoint: https://api.example.com/v1/chat/completions
  model: your-model-id
  api_key_env: QQBOT_MODEL_KEY
  timeout_seconds: 30
```

模型未配置时，机器人仍可处理 `/帮助`、`/状态` 等确定性命令。默认人格位于 `personas/elephant_default.yaml`，可通过 `persona_path` 指向自己的文件。

### OneBot v11 / NapCat

先独立安装并登录 NapCat，启用其 OneBot WebSocket 服务，并设置 Access Token。以下配置让 Core 连接 NapCat 的正向 WebSocket。

```yaml
adapters:
  - instance_id: napcat
    kind: onebot
    bot_id: 'YOUR_BOT_QQ'
    secret_env: QQBOT_ONEBOT_TOKEN
    websocket_url: ws://127.0.0.1:3001
    allowed_users: ['YOUR_USER_QQ']
    allowed_groups: ['YOUR_GROUP_QQ']
    admin_users: ['YOUR_USER_QQ']
```

`QQBOT_ONEBOT_TOKEN` 与 NapCat 配置的 Access Token 保持一致。账号和群号使用字符串。`allowed_users` 控制私聊，`allowed_groups` 控制群聊，`admin_users` 授予管理命令和研究任务权限。空白名单默认拒绝对应会话。

如使用反向 WebSocket，省略 `websocket_url`，将 NapCat 连接地址设为 `ws://127.0.0.1:8080/onebot/napcat`，并发送相同的 Bearer Access Token。URL 最后一段必须与 `instance_id` 一致。

保存配置并重启 Core，在白名单私聊发送 `/状态`，再发送一句普通问题检查模型回复。管理页可查看适配器连接状态。

### QQ 官方 Webhook

创建并配置官方机器人应用，将配置加入同一个 `adapters` 列表。

```yaml
  - instance_id: official
    kind: official
    bot_id: YOUR_BOT_ID
    app_id: YOUR_APP_ID
    secret_env: QQBOT_OFFICIAL_SECRET
    allowed_groups: ['YOUR_GROUP_OPENID']
    allowed_users: ['YOUR_USER_OPENID']
    admin_users: ['YOUR_USER_OPENID']
    full_mode_enabled: false
    full_mode_verified: false
    proactive_verified: false
```

在平台配置公开 HTTPS 回调地址 `https://YOUR_DOMAIN/webhooks/qq/official`，通过反向代理转发到 Core。仅公开所需回调路径，管理访问使用本机或 SSH 转发。白名单填写对应事件中的平台 OpenID。

全量群消息与主动投递需要实际应用权限及目标环境验收。任务结果受平台回复窗口约束，必要时等待同会话的新触发后投递。

### 其他常用字段

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `deployment_id` | `elephant-local` | 部署身份，参与持久化身份计算，已有数据的部署应保持稳定 |
| `database` | `var/core.db` | SQLite 路径，运行账号需要写权限 |
| `source_url` | 空字符串 | 对应部署版本的源码地址，发布网络服务时填写 |
| `send_budget_per_minute` | `6` | 统一发送预算，范围 1–60 |
| `timezone` | `Asia/Shanghai` | 时区 |
| `tasks_enabled` | `false` | 完成 Worker 与隔离部署后启用 |
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
| `/静默 30m`、`/恢复` | 管理员设置临时静默或恢复发言 |
| `/研究 <目标>` | 管理员提交后台研究任务，需启用 Worker |
| `/任务 [编号]`、`/取消 <编号>` | 查询任务或请求取消 |

## 后台研究与 Linux 部署

后台任务需要分别运行 Core、可信 Worker supervisor 与隔离 runner。启用 `tasks_enabled` 前需完成以下配套配置。

1. 按部署说明安装 `/opt/qqbot` 运行环境、独立服务账号、固定 root launcher 与网络命名空间。
2. 在 Core 配置 `tasks_enabled: true`、`internal_socket: /run/qqbot/internal.sock`，设置独立内部令牌。
3. 将相同内部令牌写入受限的 `/etc/qqbot/worker.token`，为 Worker 配置独立的 `QQBOT_RUNNER_OPENAI_KEY`。
4. 对齐 Worker 的 `--model`、完整 `--model-endpoint` 与 root policy 中的模型许可名单。仓库服务模板中的供应商与模型值需要按部署环境调整。
5. 完成目标主机的 `pi-isolation-check --enable-policy` 检查，配置产物目录权限，再启动 Core / Worker / 网络服务。

前台模型与 Worker 模型分别配置。生产 runner 使用 `--runner-mode systemd`，本地协议开发可使用 `local` 模式。

- [Ubuntu 安装与隔离检查](deploy/README.md)
- [任务控制与恢复契约](docs/task-implementation.md)
- [Harness 工具与产物边界](docs/harness-contract.md)
- [运维、备份与投递核对](docs/runbooks/operations.md)

## 开发与验证

```sh
uv run pytest -q
uv run ruff check src tests evals scripts
```

测试使用合成消息和本地模型夹具。部分测试会运行 `node_modules/.bin/pi`，请先安装 npm 锁定依赖。真实 QQ 收发、供应商调用和目标 Linux 主机隔离分别验收。

## 文件与隐私

凭据通过环境变量注入，本地配置放在 `configs/local.yaml`，数据库、会话记忆、人格覆盖和任务产物放在 `var/`。这些位置及常见密钥、备份、日志文件已加入 `.gitignore`。现场验收记录和截图保留在本地。

自定义目录或导入用户档案时，将实际配置、档案清单和运行数据放到仓库外，并检查 `git status --short` 后再提交。备份含用户数据，应限制权限并设定保留期。

## 许可证

原创代码采用 AGPL-3.0-only。提供网络服务时，应向使用者提供对应版本源码入口，可通过 `source_url` 配置。第三方依赖、参考项目与许可证据见 [第三方说明](THIRD_PARTY_NOTICES.md) 和 [版本锁定清单](upstreams.lock.yaml)。
