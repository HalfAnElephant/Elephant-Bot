# 第三方来源与许可证登记

初始登记日期：2026-11-08；MaiBot 移植登记更新：2026-11-09。精确版本、原文件 SHA256、目标路径与修改说明见 `upstreams.lock.yaml`。

本项目原创代码、测试、配置与脚本采用 AGPL-3.0-only，根目录 LICENSE 提供完整条款。下面登记的 MaiBot 代码、提示词和配置文案保留上游 GPL-3.0 许可与 MaiBot contributors 来源；本次没有复制上游测试夹具。

| 模块/来源 | 使用方式 | 许可与限制 |
| --- | --- | --- |
| MaiBot personality/expression | 保留备用风格兼容、表达抽样与规范化代码、学习与审核提示词，参考口语表达和群私聊提示结构 | GPL-3.0；保留来源和许可，不导入上游全局运行时或 A_memorix |
| Aurabot | 改编大肥鱼角色与打字习惯，参考主动搭话机制 | MIT；版权与许可全文见下文 |
| A_memorix | 仅核对许可 | AGPL-3.0，额外 GPL 授权只适用于 MaiBot，本项目不继承 |
| AstrBot Webhook | 协议参考 | AGPL-3.0；未复制代码 |
| pi 1.5.0 | 独立进程 JSONL RPC | MIT，安装包保留上游版权；不捆绑个人配置 |
| NapCat | 用户另行安装，经 OneBot 连接 | 自定义 Limited Redistribution License；含非商业和修改发布限制，本仓库不打包其代码/二进制 |
| OneBot v11 | 独立实现协议 | 固定快照未发现根 LICENSE，不复制其文档或夹具 |
| QQ OpenAPI | 协议调用 | 应用权限与平台条款另行适用；不代表已获全量/主动权限 |

许可证原文存于 `docs/upstream-evidence/`。锁定依赖的逐包许可清单随安装后生成；运行时分发须保留各自版权。网络服务发布时必须提供对应版本源码入口；部署配置要求明确源码地址，不能把本项目标为纯 MIT。未引入 Koishi、Hermes 的完整 Agent Loop 或 DSH 运行时；Hermes 技能子系统的移植版本见下文。

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
| `personas/elephant_default.yaml` | `src/config/official_configs.py` | 旧版曾复制默认人格文案，当前参考其口语表达及群私聊习惯，结合 Aurabot 角色分字段改写 |
| `src/qqbot/dialogue/__init__.py` | `src/chat/replyer/maisaka_generator_base.py` | 适配概率触发备用风格的选择逻辑；统一渲染旧/新配置，每个执行 run 固定一次选择 |

学习入口和同场景/表达的去重计数参考 `src/learners/expression_learner.py`；没有复制该大型模块。其原文件哈希也列入锁定清单，以区分机制参考与直接移植。Core 调度、数据库迁移、管理 API、权限和删除恢复接线属于本项目适配实现，不声称是 MaiBot 原样代码。

## Aurabot 人格与主动搭话

来源为 [aureth-labs/dsh-aurabot-qq](https://github.com/aureth-labs/dsh-aurabot-qq)，固定提交 `1057a861e4e96aa2fe050000d238280a7901b9dc`。人格 v5 曾将 `personas/dafeiyu.md` 正文改名后装入 `identity`。v6 保留角色外貌和喜好，按身份、性格、表达和群私聊场景改写，沿用用户定制的服饰，移除缺乏运行支撑的能力描述，并将宿主表达规则独立组装。主动搭话参考 `src/features/companion.js`，适配本项目消息和持久化接口。

当前 Leader 在 `src/qqbot/runtime/persona.md` 使用 2026-11-13 读取的 `main/personas/dafeiyu.md` 改编稿，源文件 SHA256 为 `70295531945326e1aa77fe26d01970124e3d011080cdadaa5e68f5f5f9c70f14`。保留“大象”称呼，采用上游外貌、性格、短句习惯和白米饭偏好，按真实能力与结果交付调整身份、生活、情绪、格式和控制标签描述。此改编稿随 Python 包分发，统一进入 Leader 的聊天、进度、提问和最终汇报提示。

```text
MIT License

Copyright (c) 2026 Aurabot contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```


## Hermes Skills 移植

来源：[NousResearch/hermes-agent](https://github.com/NousResearch/hermes-agent/tree/67664e236ddcc45824b871d575677287599af152)，固定提交 `67664e236ddcc45824b871d575677287599af152`。
Copyright (c) 2025 Nous Research，MIT License。完整许可随适配器保留于 `src/qqbot/adapters/hermes_skills/LICENSE.hermes`，原始文件 SHA256 和修改边界登记于 `_vendor/SOURCES.json`。

移植原生 skills_list、skill_view、skill_manage（operations 数组），技能路径校验、描述预算、patch 匹配、跨进程重入锁、批量快照及失败回退、变更 ledger 和 blob 回退、后台复盘来源与先读后改保护，以及官方学习/复盘提示词。导入改为包内导入；profile/home、目录、配置通过宿主 ContextVar 绑定。技能正文只存标准 SKILL.md，旧 learned_skills 数据库停止写入并提供一次性核对导出。

Elephant 负责当前身份授权、owner 目录、模型运行与统一调度；不移植 Hermes 的 Agent Loop、插件加载、项目自动发现、自动安装依赖、inline shell、Curator 定时调度或逐次审批。后台复盘保留实际 Agent 的计数与官方条件，由宿主排队，不回写原对话；技能仍是参考资料，不增加工具权限。第三方技能及其附件须单独核对许可。
