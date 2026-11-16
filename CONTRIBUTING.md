# 参与贡献

Elephant Bot 欢迎问题报告、文档修订、合成夹具和代码贡献。讨论和协作请遵守[行为准则](CODE_OF_CONDUCT.md)。如发现可能的安全漏洞或凭据泄露，请先阅读[安全政策](SECURITY.md)，不要在公开 Issue、PR、评论或日志里发布细节。

## 从 Issue 到 PR

1. 在 [Issues](https://github.com/HalfAnElephant/Elephant-Bot/issues) 搜索是否已有相同问题。报告故障时，注明版本或提交 SHA、操作系统及架构、运行方式、最小复现、预期与实际结果。较大的设计改动先在 Issue 中讨论边界和验收方式。
2. 从目标分支创建工作分支。遵循已有代码和文档布局，把无关重构拆开。修复尽量附上失败时能暴露问题、修复后能通过的回归测试。
3. 提交 PR 时关联 Issue，说明本地和 CI 分别验证了什么。涉及界面时附脱敏截图，涉及配置、数据、权限或部署时注明升级和回滚影响。没有运行的检查请直接说明。

## 本地检查

按 [README 的依赖与本地启动说明](README.md#本地启动)准备 Python、uv、Node.js、npm 与 FFmpeg。在仓库根目录运行：

```sh
uv sync --locked
npm ci --ignore-scripts
uv run pytest -q
node --test tests/console_ui.test.cjs
uv run ruff check src tests evals scripts
```

改动小范围时可先运行针对性的测试，再参考 [CI 的 Correctness 阶段](.github/workflows/ci.yml)补齐检查。配置字段有变化时，使用 CI 的命令重新生成并核对 `configs/schema.json`。涉及持久 Job、系统隔离或发行制品的改动，另按[发布与运维手册](docs/runbooks/releases.md)验收；不要用本地单测代替目标主机验证。

## 合成夹具与可分享证据

- 优先使用合成 QQ 事件、合成邮件、临时 SQLite 和模拟 HTTP 响应。先看 [tests/](tests/) 中同模块的已有夹具，不把真实消息、邮箱、账号权限或供应商密钥当作测试前提。
- 本地测试不需要真实 QQ 或 DeepSeek 凭据。CI 的真实模型回归是单独步骤；未运行真实回归时注明「未验证」，不能用模拟结果声称第三方端到端验收已通过。
- v0.1.0 用 `qqbot doctor` 做本地诊断，不支持 `--share-safe`。提交 Issue 时只摘要相关检查结果，不整段粘贴诊断输出。
- 只保留定位需要的几行错误。复制前检查并替换令牌、Authorization/Cookie、QQ 标识和二维码、邮箱地址与正文、绝对路径、内部域名和可链接到个人的数据。不要上传 `.env`、完整运行配置、数据库、对话导出或原始日志包。
- 不要把真实凭据写入测试、提交信息、截图、Issue 或 PR。若无法在公开讨论中安全提供证据，不要发布原件；涉及安全问题按[安全政策](SECURITY.md)处理。
