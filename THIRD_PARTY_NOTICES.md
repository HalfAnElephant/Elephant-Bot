# 第三方来源与许可证登记

初始登记日期：2026-11-08；MaiBot 移植登记更新：2026-11-09。精确版本、原文件 SHA256、目标路径与修改说明见 `upstreams.lock.yaml`。

本项目原创代码、测试、配置与脚本采用 AGPL-3.0-only，根目录 LICENSE 提供完整条款。下面登记的 MaiBot 代码、提示词和配置文案保留上游 GPL-3.0 许可与 MaiBot contributors 来源；本次没有复制上游测试夹具。

| 模块/来源 | 使用方式 | 许可与限制 |
| --- | --- | --- |
| MaiBot personality/expression | 复制默认人格文案、表达抽样与规范化代码、学习与审核提示词；适配表达选择和审核流程 | GPL-3.0；保留来源和许可，不导入上游全局运行时或 A_memorix |
| A_memorix | 仅核对许可 | AGPL-3.0，额外 GPL 授权只适用于 MaiBot，本项目不继承 |
| AstrBot Webhook | 协议参考 | AGPL-3.0；未复制代码 |
| pi 1.5.0 | 独立进程 JSONL RPC | MIT，安装包保留上游版权；不捆绑个人配置 |
| NapCat | 用户另行安装，经 OneBot 连接 | 自定义 Limited Redistribution License；含非商业和修改发布限制，本仓库不打包其代码/二进制 |
| OneBot v11 | 独立实现协议 | 固定快照未发现根 LICENSE，不复制其文档或夹具 |
| QQ OpenAPI | 协议调用 | 应用权限与平台条款另行适用；不代表已获全量/主动权限 |

许可证原文存于 `docs/upstream-evidence/`。锁定依赖的逐包许可清单随安装后生成；运行时分发须保留各自版权。网络服务发布时必须提供对应版本源码入口；部署配置要求明确源码地址，不能把本项目标为纯 MIT。未引入 Koishi、Hermes 或 DSH 运行时，故不虚填采用版本。

## MaiBot 表达与人格移植

来源仓库：<https://github.com/Mai-with-u/MaiBot>，固定提交 `e5391507531706a8bfce011d55ed94628039f2de`。完整上游许可证见 [maibot-license](docs/upstream-evidence/maibot-license)，与该提交的 `LICENSE` 逐字节一致，SHA256 为 `3972dc9744f6499f0f9b2dbf76696f2ae7ad8af9b23dde66d6af86c9dfb36986`。

| 本地文件 | 上游原路径 | 本地修改 |
| --- | --- | --- |
| `src/qqbot/expressions/weighted_sampling.py` | `src/learners/weighted_sampling.py` | 保留加权不放回抽样算法，增加来源/SPDX 头及保留上游代码风格的 Ruff 豁免 |
| `src/qqbot/expressions/expression_style_utils.py` | `src/learners/expression_style_utils.py` | 保留表达前缀规范化及提示示例排除，增加来源/SPDX 头及 Ruff 豁免 |
| `src/qqbot/expressions/prompts/learn_style.prompt` | `prompts/zh-CN/learn_style.prompt` | 保留提示词原文 |
| `src/qqbot/expressions/prompts/expression_evaluation.prompt` | `prompts/zh-CN/expression_evaluation.prompt` | 保留提示词正文，仅末尾空行不同 |
| `src/qqbot/expressions/__init__.py` | `src/chat/replyer/maisaka_expression_selector.py` | 适配快速档候选门槛、高频/全池抽样、去重与表达片段格式；改用本项目字符串 ID、权限、来源校验和持久化 |
| `src/qqbot/expressions/learner.py` | `src/learners/expression_utils.py` | 复用四条表达适宜性审核标准；模型传输、响应校验和来源编号映射改用本项目 HTTP/DTO 接口 |
| `personas/elephant_default.yaml` | `src/config/official_configs.py` | 复制人格、行为、回复风格、群私聊提示和备用风格默认文案；保留“大象”名称，映射配置字段并保留事实/权限/隐私边界 |
| `src/qqbot/dialogue/__init__.py` | `src/chat/replyer/maisaka_generator_base.py` | 适配概率触发备用风格的选择逻辑；统一渲染旧/新配置，每个执行 run 固定一次选择 |

学习入口和同场景/表达的去重计数参考 `src/learners/expression_learner.py`；没有复制该大型模块。其原文件哈希也列入锁定清单，以区分机制参考与直接移植。Core 调度、数据库迁移、管理 API、权限和删除恢复接线属于本项目适配实现，不声称是 MaiBot 原样代码。
