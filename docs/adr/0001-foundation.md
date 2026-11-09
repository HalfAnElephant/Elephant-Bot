# ADR 0001 — 独立内核与验证渠道

2026-11-08，接受。

空目录起步，以 Python 3.13.9、Pydantic v2、FastAPI、SQLite WAL/SQLAlchemy 2/Alembic 建立单写入 Core；Worker 只使用内部 API。固定 pi 1.5.0（Node >=22.19.0；开发机 26.12.1）。所有领域身份按部署、适配器、机器人、surface 和远端 ID 隔离，平台对象只存在于 adapter。

本轮不用上游全局运行时，不复制 MaiBot 拟合参数；采用 AGPL-3.0-only 原创实现，保留来源许可证据。NapCat 的实际安装版本与协议能力由部署方验收。

采用本地验证渠道，没有远端 CI。协议夹具与真实平台验收分开；无凭据不自动使用机器上的个人模型凭据；实际供应商测试使用经授权且按进程分离的凭据。默认 addressed_only；social、通用 shell 与未实测主动发送关闭。配置错误启动失败。

范围覆盖入口、DTO、迁移、Inbox、Outbox、配置、测试，再冻结接口供策略/记忆与任务并行开发。UNKNOWN 投递不可盲重试，过期租约不可写结果。先实现文本；媒体只规范化元信息，发送能力据实为 false。
