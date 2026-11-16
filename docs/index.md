# Elephant Bot 文档

Elephant Bot 是通过 QQ 官方接入运行的个人助手。它可以直接回答问题，也可以持久保存后台任务或定时规则。面向自己部署并管理应用的人，不是用于自动运营群聊或代替人工审核的服务。

- **查资料**：配置相应查询工具后，在当前对话查询天气或仓库 Issue；实际外部服务须分别验证。
- **交付文件任务**：把需要多步执行的任务交给后台继续处理，回报已完成状态和成果。
- **处理客观触发**：按指定条件处理邮件并产出摘要；发邮件等外部操作仍需明确授权。

## 从哪里开始

| 目标 | 入口 |
| --- | --- |
| 本地安装、配置模型与首个任务 | [README：本地启动](../README.md#本地启动) |
| 用官方 QQ 扫码绑定 | [官方扫码说明](operations/qq-onboarding.md) |
| 安装到服务器或诊断服务 | [部署手册](../deploy/README.md)、[运维与恢复](runbooks/operations.md) |
| 下载发布包、升级、备份或回退 | [发布与升级手册](runbooks/releases.md) |
| 核对配置来源、默认值、环境引用和重启要求 | [配置参考](configuration.md) |
| 设置邮箱规则、长期记忆、可复用技能 | [邮件任务](mail-trigger-drafts.md)、[OpenViking](openviking.md)、[Skills](learned-skills.md) |
| 了解后台任务、工具/执行边界 | [任务实现](task-implementation.md)、[Harness 契约](harness-contract.md)、[跨重启 Job](operations/persistent-jobs.md) |
| 复跑性能基线或核对故障恢复证据 | [控制面与混合负载](runbooks/control-plane-baseline.md)、[状态归属与契约](runtime-state-ownership.md) |
| 写代码、跑测试、提交问题或安全报告 | [贡献指南](../CONTRIBUTING.md)、[安全报告](../SECURITY.md) |

选择对应**发布版本**的文档。管理端默认监听回环地址；外部模型、QQ、邮箱、持久记忆和主机工具各有单独配置与权限要求。先做一个不依赖真实外部操作的首任务，再验收你启用的能力。平台兼容、断网行为、真实服务可用性和最终送达都不要从单一冒烟测试推断。
