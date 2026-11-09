# 第三方来源与许可证登记

登记日期：2026-11-08；登记完成后才开始业务代码实现。精确版本见 `upstreams.lock.yaml`。

原创 `src/qqbot/**`、测试、配置与脚本采用 AGPL-3.0-only，根目录 LICENSE 提供完整条款。尚无复制的上游业务代码、提示词或测试夹具；后续移植必须逐文件登记 SHA、原路径、许可、日期与修改。

| 模块/来源 | 使用方式 | 许可与限制 |
| --- | --- | --- |
| MaiBot gate/personality | 机制参考，独立实现 | GPL-3.0；未复制系数、代码或全局运行时 |
| A_memorix | 仅核对许可 | AGPL-3.0，额外 GPL 授权只适用于 MaiBot，本项目不继承 |
| AstrBot Webhook | 协议参考 | AGPL-3.0；未复制代码 |
| pi 1.5.0 | 独立进程 JSONL RPC | MIT，安装包保留上游版权；不捆绑个人配置 |
| NapCat | 用户另行安装，经 OneBot 连接 | 自定义 Limited Redistribution License；含非商业和修改发布限制，本仓库不打包其代码/二进制 |
| OneBot v11 | 独立实现协议 | 固定快照未发现根 LICENSE，不复制其文档或夹具 |
| QQ OpenAPI | 协议调用 | 应用权限与平台条款另行适用；不代表已获全量/主动权限 |

许可证原文存于 `docs/upstream-evidence/`。锁定依赖的逐包许可清单随安装后生成；运行时分发须保留各自版权。网络服务发布时必须提供对应版本源码入口；部署配置要求明确源码地址，不能把本项目标为纯 MIT。未引入 Koishi、Hermes 或 DSH 运行时，故不虚填采用版本。
