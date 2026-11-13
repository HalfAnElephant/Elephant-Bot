# OpenViking

OpenViking 是本项目的长期事实记忆后端。配置 endpoint 并提供可信的 USER 绑定后，私聊记忆默认启用，群聊记忆默认关闭。endpoint 未配置或没有匹配的绑定时，不访问远端服务。

## 配置

部署并保护 OpenViking 服务，配置其模型、存储和 API key 认证。当前适配器锁定并核对 `v0.5.4.1`。地址必须是 HTTP(S) origin；非本机服务必须使用 HTTPS。为每个 owner/workspace 分配独立的 OpenViking account、USER 和凭据环境变量。

```yaml
memory:
  openviking:
    endpoint: "http://127.0.0.1:1933"
    expected_version: "v0.5.4.1"
    private_enabled: true
    group_enabled: false
    bindings:
      - owner_id: "personal-owner"
        workspace_id: "personal"
        account_id: "your-account"
        user_id: "your-user"
        api_key_env: "QQBOT_OPENVIKING_KEY"
```

把凭据放在进程环境中，不要写入配置或版本库。群聊或项目工作区需单独配置匹配的作用域和身份绑定。运行当前数据库迁移以创建记忆操作与删除屏障账本；无需安装 OpenViking Python SDK。

## 数据流与边界

- [KnowledgeRuntime](../src/qqbot/runtime/knowledge.py) 从授权的 Pi 原生 Session 增量导出来源事件。空白、疑似密钥、工具输出、转述、摘要、技能复盘及缺少来源证据的 verified fact 不提交提炼。
- [OpenVikingMemory](../src/qqbot/adapters/openviking/service.py) 以 owner/workspace 对应的 USER 身份，串行提交同步、提炼和记忆变更。每个原始事件单独进入隔离的 OpenViking Session，显式 commit 并轮询提炼任务；这会为多个事件产生多次 commit。提炼类型限于 profile、preferences、entities、events。working memory、peer memory 与自动 commit 关闭。
- OpenViking 保存事实和索引。SQLite 保存 owner 绑定、来源 ID 与 hash、操作回执、索引水位、epoch 和更正/遗忘屏障，不保存事实正文副本。
- 检索结果只作为候选 URI。宿主核对 URI、owner、来源、索引状态和 epoch，再从 OpenViking 读取正文；当前聊天可用权限仍由宿主检查。
- 记忆读写受权限服务约束。更正与遗忘由真实用户轮次触发，先写入持久屏障，再串行处理远端变更；已撤回的来源不会重新同步。恢复旧数据库时，须导入最新删除账本并由只读核对确认远端任务、来源和 URI 后才重新开放召回。
- 远端错误会记录为降级状态，聊天继续处理；该运行时不维护本地事实库作为备用来源。OpenViking 不可用时，新记忆可能延后同步，召回也会缺少远端事实。

## 已验证范围

home 的 OpenViking `v0.5.4.1` 普通 USER 账户曾完成 16 项真实服务检查，覆盖提炼、来源、索引召回、用户隔离、重启、更正、遗忘、旧来源重放屏障及只读恢复核对。旧版固定 synthetic 基准达到 30/30 top-5 来源成员召回，当时每份文档携带整批 20 个 source ID。真实 daemon 的 HOST 入站和新 Dialogue 召回也有记录。详细数据及范围见[Leader/Workers 验收记录](leader-acceptance.md#openviking-真实组件验收)。

当前代码按原始事件隔离提炼，多事件批次会增加远端 commit 次数。已知 URI 的定向更正与遗忘保留同批其他文档，并将共享的旧来源标为 `FROZEN`，阻止旧输入重新提炼或进入活动任务重规划。旧批次缺少的精确归因仍无法从历史回执还原，显式按 source ID 遗忘会覆盖该来源的全部候选派生文档。

本批代码验证以对应 PR 的统一 CI 为准。逐事件提炼的新质量基准、真实 QQ 自动同步与跨 Dialogue 召回、活动 QQ Worker 更正或遗忘后的完整交付仍待实测。服务端备份、审计日志及供应商保留策略由各服务管理。
