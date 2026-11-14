# CI 与 GitHub Release

## 自动检查

Pull request、合并队列和 `main` 推送运行 `CI`，也可从 Actions 手动执行。
使用 Ubuntu 24.04、`.python-version` 指定的 Python、Node 22.19.0、uv 0.11.8
以及 Python/npm 锁文件。检查包含以下范围。

- Python 全量回归，包含锁定 Pi 与本地模拟模型的协议测试。缺少 Pi 或出现跳过用例时检查失败。
- `DeepSeek dialogue` 使用官方 `deepseek-flash` API 检查真实模型的对话决策、主动确认、持久会话复用与任务派发。
- 控制台 Node 测试、Ruff 和配置 schema 一致性。
- wheel、sdist 与完整源码归档构建。
- 在归档解压目录中安装生产依赖和 wheel，执行配置校验、数据库迁移/FTS5 检查、人格加载及 HTTP/静态资源冒烟检查。
- 从 Actions 重新下载上传的发布附件，校验内容校验和与提交号。

`correctness-results` 保存环境、原始日志与 JUnit，`release-assets` 保存构建产物，
均保留 14 天。任一前置检查失败会阻止后续构建或发布。
CodeQL 在 PR、`main` 推送和每周一扫描 Python 与 JavaScript/TypeScript。
Dependabot 每周分别检查 uv、npm 和 Actions 更新，更新通过 PR 接受同一套检查。
第三方 Actions 使用完整提交 SHA 固定，普通检查令牌仅授予源码读取权限。

### 真实模型对话回归

仓库 Actions Secret `DEEPSEEK_API_KEY` 提供测试凭据，调用地址为
`https://api.deepseek.com/chat/completions`。同仓 PR、`main` 推送、合并队列、
手动 CI 和发布验收会运行该检查并使用 API 额度。外部 fork PR 与 Dependabot
缺少此凭据，继续执行其余检查。

`uv run python evals/dialogue_live.py` 通过真实 Core、Pi 和 QQ 协议链路运行合成对话。
测试请求使用 `reasoning_effort=none` 关闭思考模式。
天气与 Issue 数据使用固定业务夹具，QQ 输出由本地 HTTP 接收器记录。
检查问候直接回答、查询主动确认后给出最终答复、含旧工具调用历史的会话继续处理，
以及网页请求的后台派发和接收确认。网页样本验收到派发与确认边界。

`dialogue-live-results` 保存 `test-results/dialogue-live.json`，保留 14 天，包含
首条消息、进度、最终回执的时序和耗时。消息措辞由模型生成，耗时作为观测指标。
缺少凭据、API 错误、超时或验收断言失败均使检查失败。真实 QQ 客户端收件和
后台网页成果仍需分别验收。

`uv run python evals/deepseek_tools_live.py` 使用同一凭据调用真实 Search、Fetch、Files 和 ASR API，
检查合成 PDF 的解析与模型阅读、重复上传复用，以及合成口述 SILK 的转写。临时远端文档在检查后删除。
结果保存为 `test-results/deepseek-tools-live.json`。QQ 私有 CDN 来源与凭据隔离使用确定性回归，
真实 QQ 语音接收和客户端回复仍需单独验收。

仓库分支保护建议要求 `Correctness` 和 `Build and smoke test` 成功后合并。
仓库管理员可在 GitHub Settings 的 Rules 中设置必过检查及禁止强推。

## 发布版本

1. 在同一 PR 中修改 `pyproject.toml` 的 `project.version` 并执行 `uv lock`，提交锁文件。
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

## 手动部署

从对应 Release 下载 `elephant-bot.tar.gz`、`REVISION` 和 `SHA256SUMS`。
源码包包含迁移、默认人格、前端资源、锁文件、runner 脚本及部署模板。
wheel 和 sdist 一并提供，生产安装使用完整源码包中的运行目录。

仅下载上述三个文件时，在空目录校验源码包与提交号。

```sh
sha256sum --check --ignore-missing SHA256SUMS
cat REVISION
tar -xzf elephant-bot.tar.gz
cd elephant-bot
uv sync --locked --no-dev --no-editable
npm ci --ignore-scripts
```

随后按 [Ubuntu 部署说明](../../deploy/README.md) 准备 root 控制的 `/opt/qqbot`。
更新已有主机前核对任务状态并备份，沿用主机上的配置、凭据和运行数据。
部署后检查 `/health/live`、`/health/ready`、Worker 状态和目标主机隔离。
QQ 收发与真实供应商调用按各自环境验收。

回退前保留失败现场与当前数据库备份。数据库迁移会在版本变更前生成备份，
恢复应使用 [运维与恢复](operations.md) 中的删除账本与备份流程，
同时核对运行配置和数据库 schema 与目标代码的兼容性。
