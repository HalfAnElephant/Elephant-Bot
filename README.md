# Elephant Bot

QQ 官方机器人上的个人助手。一个 Python daemon 管理持久 Leader 对话、根目标和按需 Worker；Pi 管理各 Agent 的原生历史与压缩，OpenViking 管理长期记忆，Hermes 原生工具管理标准 `SKILL.md`。

- 同一 Dialogue 可以挂多个目标；Leader 派发后结束轮次，Worker 独立推进。
- 任务、输入、操作回执和投递状态保存在 SQLite。等待会让出执行资格，取消与恢复需要真实停止证明。
- 所有 Agent 共用执行和模型请求额度。Linux 上的持久 Job 由 systemd user manager 监督，可跨 daemon 重启继续运行。
- QQ 保留四种官方 surface、媒体、按钮、Markdown、私聊流式及权限检查。实际能力取决于应用权限与平台验收。
- 长期记忆按验证过的 owner、工作区和可见范围绑定。更正与忘记使用 epoch 和删除屏障；后端故障时聊天继续，不建立备用事实库。
- Skills 使用锁定版本的 Hermes 工具、标准文件和条件复盘，Leader 与 Worker 共用各自 owner 的目录。

需要 Python 3.13、uv、Node.js >= 22.19.0 和 npm。协议开发支持 macOS/Linux；生产使用 Linux、systemd、cgroup v2 和 bubblewrap。NapCat/OneBot 接入、社交 Judge、主动插话、表达学习、persona 注入、research 专用执行器与独立 Worker API 已退役。

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

完整配置见 [示例](configs/example.yaml) 与 [Schema](configs/schema.json)。管理页支持校验、版本、保存和有限热应用；连接、身份、模型及运行边界的修改需要重启 daemon。网页配置写入启动数据库旁的 `runtime-config.json`，历史版本在 `config-versions/`。保存配置不会自动搬迁数据库或部署服务。

管理 API 使用管理员令牌，实时权限策略使用既有 PermissionService。任务详情和附件受真实持久身份、owner 和当前权限约束；管理任务可使用 `POST /api/v1/tasks`，必须提供稳定的 `Idempotency-Key`，同键不同目标会被拒绝。

| QQ 命令 | 用途 |
| --- | --- |
| `/帮助`、`/状态` | 命令与确定性服务状态 |
| `/任务 [编号]`、`/取消 <编号>` | 查询或取消任务树 |
| `/补充 <编号> <内容>` | 追加输入或回答持久化问题 |
| `/新对话 <名称>`、`/切换对话 <名称或编号>`、`/对话` | 管理持久 Dialogue |
| `/静默 [30m]`、`/恢复` | 当前会话静默与恢复 |

自然语言交给 Leader 判断直接答复或委派。任务完成、成果验证和 QQ 送达是独立状态；排队不表示已经启动，UNKNOWN 外部操作必须先核对。

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
