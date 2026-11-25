# CI 与 GitHub Release

## 自动检查

Pull request、合并队列和 `main` 推送运行 `CI`，也可从 Actions 手动执行。
使用 Ubuntu 24.04、`.python-version` 指定的 Python、Node 22.19.0、uv 0.11.8
以及 Python/npm 锁文件。检查包含以下范围。

- Python 全量回归，包含锁定 Pi 与本地模拟模型的协议测试。缺少 Pi 或出现跳过用例时检查失败。
- 控制台 Node 测试、Ruff 和配置 schema 一致性。
- wheel、sdist 与完整源码归档构建。
- 在归档解压目录中安装生产依赖和 wheel，执行配置校验、普通与脱敏诊断、HTTP/静态资源检查。用合成管理员令牌提交任务，重新启动应用后检查任务仍可查询且重复提交不新增任务；这一步不执行模型。
- macOS（Intel 和 Apple Silicon）下载 Build 上传的同一制品 ID，校验 SHA-256 与 REVISION，检查 Pi 安装确认、私有安装和复用。真实 Pi 与本地合成模型完成首任务、正常重启读回、同键重试、CLI 备份及新路径恢复。登录自启在真实桌面会话中验收。
- Ubuntu 24.04 的 GitHub 托管 VM 从同一已校验制品完成 systemd 新装、拒绝运行中重装、停机重装和重新就绪。随后以实际服务执行合成模型首任务，核对停止、重启后的原结果与同键唯一性，恢复原配置和文件权限。其他发行版及主机工具隔离按对应环境验收。
- 通过上传返回的产物 ID 下载同一份发布包，校验 SHA-256 和提交号；标签发布复用该 ID，不按名称重新选包。
- `Controlled runtime baseline` 记录三次控制面、SQLite 写竞争、0/1/10 后台任务混合负载，以及 Pi 冷热、长日志和 OpenViking 同步/召回分段。可复跑条件见[基线手册](control-plane-baseline.md)。

`correctness-results` 保存环境、原始日志与 JUnit，`release-assets` 保存构建产物，
均保留 14 天。`controlled-baseline-results` 和 `installation-*-results` 保留 90 天。安装结果包含同一 REVISION、SHA256SUMS 及首任务、重启、恢复的耗时 JSON。任一前置检查失败会阻止后续构建或发布。
CodeQL 在 PR、`main` 推送和每周一扫描 Python 与 JavaScript/TypeScript。
Dependabot 每周分别检查 uv、npm 和 Actions 更新，更新通过 PR 接受同一套检查。
第三方 Actions 使用完整提交 SHA 固定，普通检查令牌仅授予源码读取权限。

### 制品验收矩阵

每次验收使用该次 Actions 的任务结果与附件确认通过状态。Build 只构建一份源码包，后续安装任务通过其不可变产物 ID 下载。

| 环境与入口 | 同一源码包内的实际执行 | 证据与边界 |
| --- | --- | --- |
| Ubuntu 24.04，个人安装 | 安装 Python/Pi、wheel，校验与诊断，实际 Pi 首任务、重启和同键重试、CLI 备份与隔离恢复 | `Build and smoke test`，`installation-linux-results`。模型响应由本地程序生成。 |
| Ubuntu 24.04，systemd | 实际专用账号和服务单元、运行中重装拒绝、停机重装保留原文件、实际 Pi 首任务和重启读回 | `Ubuntu systemd installation`，`installation-systemd-results`。临时配置仅用于空的托管 VM，脚本最后停止服务并恢复原文件。 |
| macOS 15，Apple Silicon | 私有 Pi 安装及已有 Pi 复用两条入口，实际 Pi 首任务、重启、备份和隔离恢复 | `macOS installation (Apple Silicon)`，`installation-macos-15-results`。使用本机原生依赖。 |
| macOS 15，Intel | 同上，并从锁定源包构建 Intel 音频依赖 | `macOS installation (Intel)`，`installation-macos-15-intel-results`。 |

`smoke_guided_install.py` 使用向导共享默认配置后启动公开 CLI 与 HTTP 入口。交互菜单由向导测试覆盖，实际扫码和手机端收件由操作者完成。Debian、Fedora、Arch、其他 Linux 及 macOS 登录项的目标环境验收，应记录具体版本、SHA、安装方式和结果。QQ、邮箱与远端 OpenViking 的真实服务状态各自记录。

### 真实模型对话回归（手动）

CI 和发布验收不调用官方 DeepSeek API。需要验证真实模型链路时，操作者自行提供
`DEEPSEEK_API_KEY` 并手动运行以下脚本；调用地址为
`https://api.deepseek.com/chat/completions`。这些结果不属于 CI 通过证据。

`uv run python evals/dialogue_live.py` 通过真实 Core、Pi 和 QQ 协议链路运行合成对话。
测试请求使用 `reasoning_effort=none` 关闭思考模式。
天气与 Issue 数据使用固定业务夹具，QQ 输出由本地 HTTP 接收器记录。
检查问候直接回答、查询主动确认后给出最终答复、含旧工具调用历史的会话继续处理，
以及网页请求的后台派发和接收确认。邮箱样本保留真实邮件能力与 Core，通过自然语言创建
iCloud 普通参数配置、修改登录名、核对缺少凭据时的连接状态、追问未知邮箱标识，
并将完整中文筛选和执行要求保存为禁用草稿，账号与规则均核对 SQLite 记录。
另以受保护 API 提交合成凭据，邮箱协议由可信测试宿主注入。两个真实模型闭环分别创建
启用规则、读取摘要邮件或工单邮件、写入文件、完成归档与 Leader 验收，并通过 QQ mock
收到最终结果。报告以 `synthetic_mailbox` 标明邮箱来源，模型工具参数、答复及成果内容实际生成。
真实邮箱服务器兼容性与收发须按服务商单独验收。
网页样本验收到派发与确认边界。

脚本在本地保存 `test-results/dialogue-live.json`，包含首条消息、进度、最终回执的
时序和耗时。消息措辞由模型生成，耗时作为观测指标。缺少凭据、API 错误、超时或
验收断言失败均使手动检查失败。真实 QQ 客户端收件和
后台网页成果仍需分别验收。

`uv run python evals/deepseek_tools_live.py` 使用同一凭据调用真实 Search、Fetch、Files、OCR 和 ASR API，
检查合成 PDF 的解析与模型阅读、重复上传复用、必要时的独立 OCR 后备读取，以及合成口述 SILK 的转写。
Files 曾返回可用 `file_id`，但向模型提供空正文。验收在 `files_read_passed` 中保留 Files 阅读结果，
必要时通过 `document.ocr` 读取原件，再核验模型能否读出 OCR 中的验证码。OCR 或后备模型阅读失败均使验收失败。
`document.ocr` 返回逐页文字，`finish_reason=length` 表示该页不完整，大结果使用 `content_ref` 读取。
临时远端文档在检查后删除。结果保存为 `test-results/deepseek-tools-live.json`。QQ 私有 CDN 来源与凭据隔离使用确定性回归，
真实 QQ 语音接收和客户端回复仍需单独验收。

仓库分支保护建议要求 `Correctness` 和 `Build and smoke test` 成功后合并。
仓库管理员可在 GitHub Settings 的 Rules 中设置必过检查及禁止强推。

## 发布版本

1. 首次发布使用 `pyproject.toml` 已声明的初始版本。后续发布在同一 PR 中更新 `project.version` 并执行 `uv lock`，提交锁文件。
2. 等待检查成功并合入 `main`。
3. 从最新 `main` 创建与项目版本完全对应的标签并推送。首次版本可使用以下命令。

```sh
git fetch origin main
git tag -a v0.1.0 origin/main -m 'Release v0.1.0'
git push origin v0.1.0
```

`Release` 确认标签指向 `main` 历史上的提交、版本一致，再调用同一 CI 工作流。
发布 job 下载本次检查产物，校验 SHA-256 和提交号后创建草稿 Release、上传附件，
最后公开 Release。含字母的 Python 预发布版本会标记为 prerelease。
仅发布 job 获得 `contents: write`，使用 GitHub 自动提供的 `GITHUB_TOKEN`。

发布失败时先查看 Actions 日志。若已留下草稿，核实后删除该草稿再重跑失败 job。
已经公开的版本保留原有标签与附件，修复通过新版本发布。

## 升级与回退清单

### 升级前记录和核对

1. 记录当前/目标版本及对应的 `REVISION`，下载目标版本的配套附件并校验。确认目标代码、配置 schema、Python/Node/Pi 锁定版本和本机部署方式相容。**当前服务使用的名称和路径以该版本及本机配置为准**；二进制、用户、服务名、环境变量名不能视为原地迁移承诺。
2. 对照目标版本的示例和 schema，核对被移除的设置、实际生效的配置及依赖，先修复校验报错，不把新模板覆盖到原配置。改变权限、挂载或外部账号配置需另行确认。
3. 暂停新任务入口，等待已有任务、外部操作和发送状态可核对，再停止服务。存在不明外部操作、未确认停止的任务或无法备份的数据时，停止升级，不用重启替代核对。
4. 按[运维与恢复](operations.md#备份和隔离恢复演练)保存数据库快照、最新删除账本及独立的配置/状态备份。确认旧代码与运行依赖也可以取回。不要把备份、实际凭据或恢复日志放入发布附件。

### 升级后与回退

- 在隔离实例先验证配置可读、迁移成功、健康检查和合成首任务完成，重启后仍读得到结果。再在目标环境分别验证已启用的模型、QQ、邮箱及隔离；无凭据或未实际测试时写明“未验证”。
- 启动前出现校验、迁移或密钥错误，保持停止，保留出错现场与备份；不要多次启动覆盖证据。迁移若已经执行，不直接拿旧程序打开新 schema，也不要以清空凭据或关闭屏障换取就绪。
- 只有在目标版本的兼容说明明确支持旧程序读现有数据时，才考虑只回退运行包。否则用旧兼容版本的快照向**新路径**恢复，并应用当前最新删除账本；先确认时间窗口内的数据损失和外部操作。无法确认账本、schema、停机证明或真实发送状态时，停止回退，转人工核对。
- 对已经发布的版本，在附注记录验证实际覆盖的平台、制品 SHA、配置废弃项、迁移、回退条件与没有验证的组合。确认检查作业成功不等于真实 QQ 送达，也不替代单独的合并门槛。

## 手动部署

从 [GitHub Releases](https://github.com/HalfAnElephant/Elephant-Bot/releases/latest) 的对应版本下载 `elephant-bot.tar.gz`、`REVISION` 和 `SHA256SUMS`。
源码包包含迁移、默认人格、前端资源、锁文件、runner 脚本及部署模板。
wheel 和 sdist 一并提供，生产安装使用完整源码包中的运行目录。

v0.1.0 发布包没有 `install.sh`。使用发布包内配套说明；开发分支上的新安装入口须使用包含该文件的检出或归档。

仅下载上述三个文件时，在空目录校验源码包与提交号。先取出这两个文件各自的唯一校验项；归档或提交号条目缺失或重复时不要继续。Linux 可使用 `sha256sum --check`，macOS 使用系统的 `shasum`。校验失败不要解压。先从 GitHub 发布标签的提交页复制完整提交 SHA，填到下方的 `EXPECTED_REVISION`；不要从同一个下载包反抄 `REVISION`。

```sh
EXPECTED_REVISION='填写发布标签所指向的完整提交 SHA'
awk '
  $2 == "./elephant-bot.tar.gz" || $2 == "elephant-bot.tar.gz" { archive++; print }
  $2 == "./REVISION" || $2 == "REVISION" { revision++; print }
  END { if (archive != 1 || revision != 1) exit 1 }
' SHA256SUMS > selected.sha256 || exit 1
if command -v sha256sum >/dev/null 2>&1; then
  sha256sum --check selected.sha256 || exit 1
else
  shasum -a 256 --check selected.sha256 || exit 1
fi
if [ "$(cat REVISION)" != "$EXPECTED_REVISION" ]; then
  printf '%s\n' '发布提交号与标签不一致。' >&2
  exit 1
fi
tar -xzf elephant-bot.tar.gz
cd elephant-bot
uv sync --locked --no-dev --no-editable
npm ci --ignore-scripts
```

随后按 [Ubuntu 部署说明](../../deploy/README.md) 准备 root 控制的 `/opt/elephant`。
使用发布包时，将已校验的 `elephant-bot` 完整目录放到 `/opt/elephant`，继续部署说明中的依赖安装、账号、配置和服务初始化步骤。
更新已有主机前核对任务状态并备份，沿用主机上的配置、凭据和运行数据。
邮箱更新同时保留数据库旁的私有 `mail-credentials/` 和原 `ELEPHANT_CREDENTIAL_KEY`，
恢复后通过账号测试核对当前修订的 IMAP/SMTP 认证状态。配置与启用步骤见[邮箱与收信任务](../mail-trigger-drafts.md)。
部署后检查 `/health/live`、`/health/ready`、Worker 状态和目标主机隔离。
QQ 收发与真实供应商调用按各自环境验收。

回退前保留失败现场与当前数据库备份。数据库迁移会在版本变更前生成备份，
恢复应使用 [运维与恢复](operations.md) 中的删除账本与备份流程，
同时核对运行配置和数据库 schema 与目标代码的兼容性。
