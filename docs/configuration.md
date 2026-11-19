# 配置来源与生效规则

本页对应当前开发分支。已发布的 v0.1.0 使用 `qqbot` 命令和 `src/qqbot` 包。当前分支使用 `elephant`。安装、服务名和目录的变动见[部署说明](../deploy/README.md#existing-installations)。升级、备份与隔离恢复沿用[发布手册](runbooks/releases.md#升级与回退清单)。

## 查询完整字段与默认值

字段类型、默认值、范围、枚举、必填项和废弃标记由随安装版本生成的 JSON Schema 提供。命令从完整发布目录运行，`--config` 使用实际启动 YAML 的绝对路径。

```sh
.venv/bin/elephant schema --config "$INSTANCE_CONFIG" > settings.schema.json
.venv/bin/elephant validate --config "$INSTANCE_CONFIG" --env-file "$INSTANCE_ENV"
.venv/bin/elephant doctor --config "$INSTANCE_CONFIG" --env-file "$INSTANCE_ENV" --share-safe
```

管理台的设置页读取同一份 Schema。查询单个字段时，顶层字段在 `properties`，嵌套配置类型在 `$defs`。例如模型窗口在 `$defs.ModelConfig.properties.context_window`，执行环境在 `$defs.ExecutionConfig.properties`。Schema 使用对应版本的模型生成，更新配置时以目标安装版本的输出为准。

`validate` 检查配置结构并给出缺项提示。`doctor --share-safe` 输出允许分享的离线状态摘要，模型、QQ 和 OpenViking 的真实连通及送达状态各自保留 `not_tested`。完整配置、运行时快照、历史配置和环境文件应按私密资料保存，其中可能包含用户偏好、账号标识、地址与本机路径。

## 从启动文件到实际生效

1. `--config` 指定启动 YAML，未指定时为 `configs/example.yaml`。缺失字段取 `Settings` Schema 中的默认值。相对路径相对于进程工作目录解析，正式部署使用绝对路径。
2. 从**启动 YAML 的 database 所在目录**读取 `runtime-config.json`。该文件按顶层字段覆盖启动配置。例如快照里的 `model` 会替换整个 `model` 对象，缺失的模型子字段重新取 Schema 默认值。启动 YAML 的同名字段修改会被已有快照覆盖。
3. 创建 Core 时再次读取上述配置，作为本进程的生效设置。管理台 `GET /api/v1/config` 的 `config/version` 表示生效配置，`saved_config/saved_version` 表示下次启动配置。两者不同时返回 `restart_required=true`。
4. 权限单独有持久化策略版本。数据库中已有策略时，它优先于启动配置中的 `permissions`。管理台权限页和 `GET /api/v1/permissions` 返回当前策略与版本。

`runtime-config.json` 的锚点始终来自启动 YAML，即使快照保存了新的 `database` 位置也保持原锚点。修改数据库位置属于迁移操作，须保留原启动 YAML、快照及目标数据的一致性。先通过设置页比较已保存与生效配置，勿凭修改 YAML 后的文件内容推断生效状态。

## 配置分组

下表覆盖 `Settings` 的全部顶层字段。表内列出作用和生效边界，逐字段默认值及约束使用上面的 Schema 查询。

| 分组与字段 | 来源和用途 | 环境引用 | 修改后的生效方式 |
| --- | --- | --- | --- |
| 基础 `deployment_id`、`database`、`source_url`、`timezone` | 启动 YAML、运行时快照及 Schema 默认值。标识、持久化位置、公开源码地址和时间计算。 | 无通用环境覆盖。 | 保存后重启。数据库与身份变更另按迁移流程核对。 |
| 入口 `admin_token_env`、`internal_token_env`、`internal_socket` | 配置保存变量名和兼容路径。旧 Worker HTTP 路由已删除，`internal_socket` 仍用于遗留 socket 清理。 | 管理认证从 `admin_token_env` 指定变量读取令牌。 | 修改变量名或路径后重启。 |
| 开关 `tasks_enabled`、`interactive_execution_enabled` | 显式启用后台目标和交互执行。裸 `Settings` 的后台任务默认关闭。 | 无通用环境覆盖。 | 保存后重启 Core 和 Worker。 |
| 模型 `model` | endpoint、模型名、窗口、超时及密钥变量名。endpoint 使用完整 Chat Completions URL。 | `model.api_key_env`。 | 启用任一执行开关时须重启 Core 和 Worker。两开关都关闭时可通过管理 API 热应用。 |
| Agent `execution`、`orchestration`、`context` | 执行环境、任务预算、模型并发、Pi 路径、会话绑定和上下文压缩。空压缩阈值按模型窗口与输出预算计算。 | Pi 可执行路径与运行环境按部署入口准备。 | 保存后重启。 |
| 渠道 `adapters` | 官方 QQ 的实例、传输、平台权限、限额和凭据引用。每个 instance_id 唯一。 | `adapters[].secret_env`，或加密 `credential_ref`。 | 普通配置保存后重启。QQ 扫码绑定入口有独立的受验证应用流程。 |
| 用户 `owner` | 本机 owner、QQ 绑定和显式偏好。 | 无通用环境覆盖。 | 普通配置保存后重启。扫码绑定由专用流程处理。 |
| 记忆 `memory` | OpenViking endpoint、owner/workspace 到 USER 的显式绑定、可见范围及预算。endpoint 和绑定齐全后才可用。 | `memory.openviking.bindings[].api_key_env` 指向各自 USER 的密钥，配置另保留 `memory.openviking.api_key_env`。 | 保存后重启。恢复后的远端状态另走删除屏障核对。 |
| 技能 `skills` | Hermes 提供者、存储根目录、群可见性和后台复盘开关。 | 无通用环境覆盖。 | 保存后重启。技能文件通过现有技能接口维护。 |
| 权限 `permissions` | 初始策略来自配置，已有数据库策略优先。 | 无通用环境覆盖。 | 权限页按 policy_version 更新后立即生效。仅保存配置中的初始策略不会替换已有数据库策略。 |
| 发送 `send_budget_per_minute` | 出站发送预算。 | 无通用环境覆盖。 | 可热应用。通过“保存”操作提交时仍等下次启动。 |

设置页的“校验”“保存”“应用”分别对应配置校验、持久化下次启动配置和受限热更新。`/api/v1/config/save` 保存完整配置并记录 `config-versions/<SHA>.json` 历史，不修改运行中的 Core。`/api/v1/config/apply` 只接受允许热更新的字段，存在待重启快照时拒绝热应用。历史版本恢复同样需要按配置版本保存，并按接口返回值重启。

## 凭据环境与版本兼容

CLI 只在显式传入 `--env-file` 时读取 dotenv 文件。它不执行 shell 代码，不展开 `${...}`，且已有进程环境变量优先于文件。YAML 中的 `*_env` 保存变量名，YAML 本身不执行环境变量插值。向导使用私有 `credentials.env`，Linux 服务和 macOS 登录入口按各自部署方式加载凭据。

标准凭据变量在 `ELEPHANT_` 与旧 `QQBOT_` 前缀间兼容读取。支持的后缀为 `ADMIN_TOKEN`、`CREDENTIAL_KEY`、`GITHUB_TOKEN`、`INTERNAL_TOKEN`、`MODEL_KEY`、`OFFICIAL_SECRET`、`OPENVIKING_KEY` 和 `RUNNER_OPENAI_KEY`。优先使用配置指定的名称，只有该名称不存在时才找兼容别名。显式空字符串会保持空值。自定义变量名按配置原样读取。

QQ 和邮箱的加密凭据还依赖原来的 Base64 32 字节 `ELEPHANT_CREDENTIAL_KEY` 或兼容旧变量。保留原密钥、凭据文件、账号绑定及数据库。向导检测到已有加密凭据而缺少有效密钥时会拒绝生成替代密钥。改模型服务会另存新密钥引用，并保留旧值供原配置回退。配置历史保存的是引用名，密钥本身随私有环境文件独立备份。

当前版本保留废弃配额的解析，但 `execution.max_active/sustained_limit`、`orchestration.pending_inputs_per_owner/pending_root_tasks_per_owner/child_parallelism/max_task_depth/max_nodes_per_root` 已不控制准入，显式写在启动 YAML 中会告警。模型并发使用 `orchestration.model_request_limit`，单任务预算使用 `execution` 中的 budget。OneBot/NapCat 配置及旧候选技能配置会被拒绝，迁移到官方 QQ 和现有 Hermes 字段后再启动。

`orchestration.leader_steering_dialogues` 接受逻辑会话 ID 列表，默认为空。开启后，会话内的纯文本补充可在 Pi 的请求或工具边界进入正在运行的用户轮次。Leader 通过 `task_outcome.covered_input_ids` 确认本轮已经处理的补充。独立请求以及附件、结构化输入留在队列中。每轮最多接纳 8 条补充，发布竞争最多重做 2 次。试点会话在正文定稿前可以显示 QQ「输入中」，正文经事务提交后发送。有有效回复上下文时，Delivery 可用 QQ 原生流式终帧投递。

空闲进程缓存数量由 `execution.warm_idle_max` 决定，设为 `0` 关闭缓存，保留时间由 `warm_ttl_seconds` 决定。`warm_max` 保留兼容，当前任务领取流程会用 `warm_idle_max` 覆盖缓存上限，因此关闭缓存应设置 `warm_idle_max=0`。忙进程按任务动态创建，模型请求按 `model_request_limit` 等待容量。实际内存随活动 Pi 进程数和原生会话大小增长，部署资源用[控制面与混合负载基线](runbooks/control-plane-baseline.md)测量。

## 安装路径与执行权限

| 入口 | 初始配置与身份 | 文件和网络边界 | 隔离不可用时 |
| --- | --- | --- | --- |
| `elephant setup` 新建配置 | 自定义权限策略默认拒绝，补齐当前 owner 所需规则。新配置的 Leader 和 Worker 都保存为 sandbox。需要 host 时须显式修改对应配置并重新验证权限。 | sandbox 工具通过既有执行和能力接口授权。host 工具拥有运行账号可访问的宿主范围，仍受业务工具权限检查。 | 相关工具报告环境不可用。修复隔离依赖或显式修改执行配置后再验收。 |
| 仓库 `configs/example.yaml` | 个人开发示例保留 allow_all，Leader 为 host、Worker 为 sandbox。 | 直接采用示例即采用这些权限。正式运行前核对生效配置与执行账号。 | sandbox 操作按执行环境报告错误。 |
| Linux 系统安装 | 使用专用非 root `elephant` 服务账号。系统安装动作由安装器执行，服务启动需单独选择。 | 服务单位、bubblewrap、systemd user Job 和工具权限共同限制可访问范围。host 能力取决于服务账号及管理员已配置的宿主权限。 | 部署预检失败需修复对应依赖或选择受支持配置。 |
| macOS 本地安装 | 当前用户运行，前台或显式选择 LaunchAgent。 | 当前支持矩阵承诺本地运行。Linux sandbox 与持久 Job 的能力按[部署说明](../deploy/README.md)区分。 | 保留配置指定的执行环境。需要本机工具时明确配置 host 并核对权限，未测能力在诊断中保持未测试。 |

内置 `serve` 监听 `127.0.0.1:8080`。`doctor --share-safe` 的监听摘要只覆盖该入口。采用自定义 ASGI 启动或反向代理时，须从对应服务启动参数核对实际监听和访问控制。

## 对已有部署的核对

1. 记录正在使用的代码 SHA、CLI、服务名、启动参数、环境文件、启动 YAML 和数据库位置。从管理设置页确认生效与已保存版本，从权限页确认当前策略。
2. 保存 YAML、`runtime-config.json`、`config-versions/`、凭据文件及加密密钥，并按[运维手册](runbooks/operations.md#备份和隔离恢复演练)独立保存数据库与最新删除账本。Pi 会话、工作区和外部 OpenViking 数据分别按其边界保存。
3. 使用新的完整发布目录，按目标版本校验配置。向导只修改自己负责的模型设置，保留无关快照字段。服务运行中拒绝修改模型或凭据。已有生产部署的服务名、用户和路径沿用该部署安排。
4. 在隔离副本完成迁移、读取历史结果、同键重试及重启读回，比较原配置与凭据内容和权限。升级使用原密钥，回退按 schema 兼容说明处理。真实模型、QQ、邮箱及远端记忆各自记录实际验收结果。

对应回归入口为 `tests/integration/test_console_configuration.py`、`tests/test_setup_wizard.py`、`tests/test_credential_env.py`、`tests/test_legacy_upgrade.py`、`tests/test_first_run.py` 与 `tests/test_deployment_contract.py`。这些受控检查覆盖配置保存、锁、兼容读取和迁移不变量，真实账号与原部署切换按目标环境另验收。
