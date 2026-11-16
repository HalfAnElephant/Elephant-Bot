# 运行时状态归属与调用边界

本页是路线图 #19 的第一步。基线为 `984b91f`（已包含邮件接入 #13）。目标是让依赖与状态写入口可见，不合并已经具有不同恢复含义的状态，也不改变公开 API 或部署架构。

## 装配、协调与生命周期

```text
app.create_app
  ├── 构造适配器与 Core；挂载公开/管理 API
  └── Core
      ├── 装配 TaskService、TaskGraph、SessionService、Leader 与各后端
      ├── 处理入站、构造有来源的请求与任务上下文
      └── LoopSupervisor 管理后台循环，新旧循环不重复拥有同一个队列

有认证的用户请求 / 管理任务 / 定时触发
  -> 宿主校验请求身份与来源
  -> TaskGraph.submit 保存目标与子项
  -> Scheduler / RuntimeClient -> TaskService.claim / renew / submit
  -> Worker 使用 Harness 与 Execution，按 lease/fence 回传，生产不走旧 HTTP 路由
  -> Leader 处理结果与协调；Outbox 独立处理平台投递
```

邮件触发也使用现有 TaskGraph，但保留邮件来源、规则与回执边界。和定时器共享任务提交不等于共享触发游标。Pi Harness 是模型、工具协议和原生 Session 的执行适配；Execution 负责进程、沙箱、工作区和系统 Job；Worker 编排一次已领取的执行，不决定用户身份、授权或目标树。

## 写入口与事实来源

| 责任 | 主写入口与事实 | 不能从它推断 |
| --- | --- | --- |
| 入站与会话 | `Inbox.accept` 保存单份原始事件；`SessionService` 管理 Dialogue、输入队列和会话归属；Pi 维护原生历史。 | 接收不意味着任务已运行、已投递或用户已授权后续动作。 |
| 目标与任务树 | `TaskGraph.submit` 创建目标和子项并记录请求键；Leader 协调、解释结果，不代替执行回执。 | 根目标的完成约束与单个尝试退出不同。 |
| 尝试与容量 | `TaskService.claim/renew/submit` 维护 run、当前租约、fence 和终态提交；Scheduler 只提供调度和模型容量。 | 租约过期不是进程停止；旧 fence 不能提交新结果。 |
| 执行与接管 | Harness/Execution 给出运行和停止证据；Worker 的 spool 暂存待确认结果；`RuntimeRecovery` 读取停止证明并复验后接管。 | spool 或进程退出不能单独证明外部动作成功。 |
| 外部效果 | ToolGateway 受宿主授权，操作账本记录 intent、dispatch、确认或未知；恢复可以只读核对。 | `UNKNOWN` 不等于失败；不确认就不能重发。 |
| 平台投递 | Outbox 独立保存发送意图与平台结果；Admin/Leader 状态是读取投影。 | 本地排队或执行成功不等于 QQ 已收到。 |
| 记忆与技能 | KnowledgeRuntime 确定来源和 scope；OpenViking 持有长期事实；SQLite 保留操作与撤销屏障；Hermes 技能保持自己的文件与版本。 | 提炼不授予业务权限；删除屏障不能由旧 Session 或旧备份绕过。 |

## 第一条迁移切片

把已存在的 QQ 适配器、Skills 设置读取与 Skills provider 从 `Core` 构造后补属性改为 `TaskService` 构造参数。传入的是单一现有适配器集合和宿主函数，不建立另一个注册中心。Skills 设置读取保持按需，以便仍按当前配置与 scope 检查；没有注入的独立 TaskService 保持无相应能力的默认行为。TaskGraph 的建立、租约事务、取消、权限模型、邮件授权和停止证明不在这一切片变更。

随后将 `MailRuntime.authorize_context` 也作为宿主构造参数传入。MailRuntime 构造时不执行 I/O 或请求 TaskService；构造顺序变化不替代实际调用时的两处授权复核。Recovery 构造需要已存在的 TaskService，因而停止证明仍在 Recovery 就绪后绑定，不为消除所有后赋值引入协调层。

验收：核对一体化构造和独立服务默认行为，再跑 QQ 富消息/文件、Skills、任务 claim/renew/result、停止/恢复和邮件相关权限回归。不把 #18 的合成控制面时间拿来证明这些业务链路等价。

## Worker 调用边界

`WorkerRequest` 是进程内请求格式，不是 HTTP 服务。旧 `internal_router` 未被生产 App 挂载，只为测试保留，已随测试迁移删除。测试仍调用实际的 `RuntimeClient`，没有另建兼容调度器；独立 TaskService 用例只替换没有启动的 Leader 队列等宿主依赖。

| 面 | 可共享 | 必须单独验证或保留 |
| --- | --- | --- |
| 已领取 Worker 的常用操作 | 请求字段、TaskService 的 fence/lease 校验、结果和副作用规则。 | 进程内测试覆盖有效、旧 fence 和错误执行者；公开 HTTP 入口返回 404，不再测试废弃路由的 bearer。 |
| 领取与等待 | 已存在的 Worker claim 服务与生产轮换。 | 保留 Worker 原生协议字段与状态；不把旧 HTTP 长轮询当作调度需求。 |
| QQ 文件 | `local_file_upload` 检查任务和内容并走现有存储。 | 保留大小、MIME、去重、取消清理、过期 lease 与分块上传回归；旧 HTTP 请求头、专属并发门禁和超时不是现行生产约束。 |

## 兼容项处置清单

| 配置 / 工件 | 当前用途 | 去留规则 |
| --- | --- | --- |
| `execution.max_active`、`execution.sustained_limit`、`orchestration.pending_inputs_per_owner`、`orchestration.pending_root_tasks_per_owner`、`orchestration.child_parallelism`、`orchestration.max_task_depth`、`orchestration.max_nodes_per_root` | 配置模型标注废弃；不能把这些值当成当前真实准入上限。 | `validate/doctor/serve` 仅对启动 YAML 中显式填写的旧字段告警，指向仍有效的模型容量/任务预算；旧配置保留解析。发布确定的迁移窗口后再删除。 |
| `internal_token_env`、`internal_socket` | 旧 Worker 监听不挂载，但管理配置仍展示字段，CLI 也借用旧 socket 路径清理残留。 | 先核对清理兼容、管理界面和升级脚本。未确认没有部署依赖前不删，不把内部命名理解为应启动新监听。 |
| `tasks_enabled`、`interactive_execution_enabled`、Worker protocol 版本、Alembic 和旧运行回执 | 当前启用边界或旧数据能否恢复所需。 | 不当作冗余字段删除；修改要有升级/回滚和旧回执读取证明。 |
