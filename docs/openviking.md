# OpenViking（可选）

QQ 官方 Bot 私聊本身保持默认接收和响应，仍受已有白名单、权限与静默模式约束。
新增的 OpenViking 私聊会话在配置服务地址后默认开启；群聊默认关闭。
未配置地址时不连接外部服务，不影响原有本地记忆。

## 配置

先独立部署可信的 OpenViking HTTP 服务，并配置服务自身的模型、存储及访问控制。
本项目不自动安装服务、不生成凭据，也不自动把历史数据库全量迁移到外部。

```yaml
memory:
  openviking:
    endpoint: "http://127.0.0.1:1933"
    api_key_env: "QQBOT_OPENVIKING_KEY"
    private_enabled: true
    group_enabled: false
    namespace: "elephant"
    timeout_seconds: 2
    operation_budget_seconds: 8
    max_records: 64
```

在运行环境设置 `QQBOT_OPENVIKING_KEY`，不要将密钥写入配置或版本库。
非本机服务要求 HTTPS；地址只能是 origin，不能带账号、路径、查询参数。
配置地址意味着允许将下述已授权文本发送到该服务，请仅使用自己信任的部署。
升级运行项目现有 `migrate` 命令，迁移 0015 添加清理账本；无需安装 OpenViking Python SDK。

## 已接入范围

- 每个部署、适配器/机器人、会话、用户分别派生隔离目录和会话 ID。
- 每轮发送前同步最近最多 16 条已授权用户/助手文本及当前输入，单条最多 4000 字符。
- 最多同步 64 条当前有权读取的本地有效记忆；上限可配置为 128。
- 调用官方 HTTP `content/batch-write` 建索引，`search/find` 提供语义召回。
  只接受当前目录中已知记忆的精确 URI，并从本地读取正文；远端返回的正文不是可信事实。
- 私聊默认参与，群聊须显式开启；频道暂不导出。权限检查包含
  `memory.external.read`、`memory.external.write`、历史读取及来源读取权限。
- 会话采用有界快照替换，不是无限追加日志。禁用自动 commit 和所有自动记忆提取，
  避免外部派生记忆脱离本地撤回、忘记和权限控制。图片、文件、工具输入和 persona 不导出。
- 远端故障、超时、异步索引尚未完成时继续使用原有本地记忆。不会阻止正常回复。
- `/状态` 展示 `openviking_status` 与 `openviking_private_sessions_enabled`。

## 清理和边界

忘记、来源撤回、权限变更、内容过期后，维护任务清理已知远端快照；控制命令也触发检查。
失败保留本地账本并重试。超时写入不直接重放追加操作，而先删除确定的快照 ID 后重建。
维护任务需要运行；后台关闭时不会自动轮询。数据库丢失后不能自动枚举和清理旧数据。

停用前，保留地址与密钥，设置 `private_enabled: false`、`group_enabled: false`，
让维护任务完成清理，再移除地址。直接换地址、namespace、deployment_id 或移除凭据，
旧服务的数据不能靠新服务清除；状态提示需要清理，应在旧服务管理端删除对应目录和会话。
服务端备份、审计日志及供应商保留策略不在本项目删除保证内。

这是受控外部索引和会话同步支持，不是把 OpenViking 的自主记忆提取结果直接合入本地事实库。
未经真实 OpenViking 部署联调；协议测试使用模拟 HTTP。参考当前官方接口：
[Sessions](https://github.com/volcengine/OpenViking/blob/main/docs/en/api/05-sessions.md)、
[Retrieval](https://github.com/volcengine/OpenViking/blob/main/docs/en/api/06-retrieval.md)、
[Content](https://github.com/volcengine/OpenViking/blob/main/docs/en/api/12-content.md)。

### 协议兼容与持久化说明

本集成依据 OpenViking 当前 main 文档和接口实现进行适配，尚未锁定或实测特定发行版。
部署前必须对目标版本确认 `memory_policy` 和显式 `auto_commit_policy: null` 的支持及服务端行为。
代码不调用 session commit，不轮询提取任务，也不把任务受理 ID 当作记忆提取完成。
服务端 session 创建和 messages/batch 成功响应仅表示本次会话写入请求成功，
不保证远端磁盘持久化、归档或长期备份。会话随下一快照被替换，不能当作永久聊天备份。
资源索引使用 `wait: false`，查询可能暂时命中不到新记录，届时使用本地召回。
