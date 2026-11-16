# 运行时状态归属与调用边界

本页对应路线图 #19。#23 已完成依赖显式装配与旧 Worker HTTP 测试迁移，#25 补充管理台读取权限。下表按这些合并后的真实入口说明状态写入、事务与恢复契约。

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


## 事务边界与调用变化

| 写入范围 | 入口与提交边界 | 中断后的事实 |
| --- | --- | --- |
| 用户输入与任务树 | `SessionService.enqueue` 保存 Dialogue/Input。`TaskGraph.submit(conn, ...)` 使用调用方事务写 root、children 和接纳事件。管理入口 `Core.enqueue_admin_task` 同一事务保存结构化来源。 | 事务提交前全部回滚。重试复用来源及 dispatch key。 |
| 领取与续租 | `TaskService.claim` 在一个事务里保留容量、增加 fence、写 run。`renew` 经 `_lease` 检查 worker/fence 后更新租约。 | 领取后的崩溃须提供停止证明。租约过期不能自行释放仍被进程占用的资源。 |
| Worker 结果 | `Worker.save_result` 先保存 spool。`TaskService.submit` 在事务内重验 lease、来源、结果证据与停止条件，写任务/尝试结果及后续事件。 | spool 在回执确认后清理。恢复读取原结果，未知副作用留在核对状态。 |
| Leader 结算 | `LeaderRuntime.complete` 在调用事务内结算 attempt、消费输入、更新目标并记录通知。 | 输入消费与结算一起提交，重复结果不能产生第二次有效交付。 |
| 工具副作用 | `SQLiteActionLedger.prepare/transition` 分别提交意图和状态转换。远端执行在两个提交之间。`ToolGateway` 及宿主授权在执行前复核身份、来源和动作。 | 远端成功、本地无回执时保留未知态，通过只读核对收敛。 |
| 平台投递 | `Outbox` 在业务事务中保存发送意图，平台调用后独立记录回执。 | 任务完成、入队、平台接受分别保留。未知投递按已有核对流程处理。 |
| 运行接管 | `RuntimeRecovery` 先读进程/原生会话/spool 证据，再经 `_commit` 或 TaskService 的原事务入口提交。 | 缺失停止证明时保留 run、session writer 和容量归属。 |

#23 前后的调用关系如下。`WorkerRequest` 的字段与 `RuntimeClient.call` 的操作名保留。

```text
修改前
Core -> TaskService(...)
Core -> tasks.qq_adapters / skills_settings / skills_provider / action_authorizer 后赋值
测试 -> 未挂载的 internal_router -> TaskService
生产 -> RuntimeClient -> TaskService

修改后
Core -> TaskService(..., qq_adapters, skills_settings, skills_provider, action_authorizer)
测试 / 生产 -> RuntimeClient -> TaskService -> claim / renew / submit
Core -> RuntimeRecovery(tasks) -> tasks.stop_verifier
```

`RuntimeRecovery.stopped` 仍在 Recovery 创建后绑定，因为它读取已创建的 TaskService。其余耦合通过已有实际依赖维护。

| 边界 | 必须保留的依赖与检查 |
| --- | --- |
| `TaskService` | `context_validator`、`action_authorizer` 与 `stop_verifier` 复验当前授权及停止证据。`submit` 验证 receipt、输入修订及证据引用后持久化。 |
| `LeaderRuntime` | `lease/authorize` 限制当前 attempt，`task_tool` 经过 TaskGraph，`complete` 同时处理输入消费与目标结算。 |
| `SessionService` | `owner/validate_workspace` 确认身份及项目。`claim` 重验保存的上下文与单 session writer 归属。 |
| `RunGateway` | 通过宿主 RuntimeClient 检查 authority、权限和额度。工具结果由 ToolGateway 与操作账本处理。 |
| `RuntimeRecovery` | 停止证明、原生结果、spool 与外部操作记录共同决定恢复路径。只有证明齐全才释放或结算。 |

## 记忆的四个边界

正式事实与远端索引由 OpenViking 持有。SQLite 记录来源、操作、水位、版本与屏障，便于重试及恢复。API 的 DTO 与异步接口以 [`MemoryPort`](../src/elephant/contracts/memory.py) 为准，后端绑定、同步驱动和恢复核对由 OpenViking 适配器实现。

| 边界 | 入口与不变量 | 可追溯回归 |
| --- | --- | --- |
| API | `KnowledgeRuntime.prepare` 召回并逐条读取。`invoke` 把更正/遗忘绑定到已认证 Leader 原始工具调用。`OpenVikingMemory.correct/forget` 先记录变更与 epoch。读取失败可降级，来源及变更授权仍须核验。 | `tests/contracts/test_memory_boundaries.py` 的普通用户轮次接纳与遗忘竞争。`tests/test_committed_memory_recovery.py` 的变更注册到宿主确认间隙。 |
| 范围 | `scope` 固定 owner/workspace/audience，`_binding/_owner` 核验独立远端身份，`read` 检查 URI 与来源回执。群和项目没有有效绑定时不能取得私人召回。 | `tests/contracts/test_memory_boundaries.py` 的群入口。`tests/test_openviking.py` 的 group/project namespace 与 foreign hit。`tests/test_project_scopes.py` 的冻结项目。 |
| 同步 | `KnowledgeRuntime.sync` 从原生来源按页导出。适配器保存 operation/source/stream，分别记录 accepted、extracted、indexed。后台 `reconcile` 复核原操作。前台普通聊天可独立运行。 | `tests/test_openviking.py` 的 offline/lost receipt/unknown append。`tests/test_memory_provenance.py` 的队列满时旧操作排空、分页及重启。 |
| 恢复 | `storage.operations.restore` 应用最新删除账本并旋转旧 session。`restore_barriers` 设置 `RESTORE_REQUIRED`。`reconcile_restore` 只读核对后开放，未知或冲突保持屏障。`KnowledgeRuntime.valid` 拒绝旧 epoch 上下文。 | `tests/test_memory_restore.py` 的快照及最新账本。`tests/contracts/test_memory_boundaries.py` 的外部旧事实重新出现。`tests/test_openviking.py` 的恢复核对。`tests/test_agent_runtime.py` 的重新规划。 |

## 行为验收与回退范围

| 行为 | #23 前后契约 | 维护时对应检查 |
| --- | --- | --- |
| 当前 worker/fence 可调用，错误 worker/旧 fence 被拒绝 | 生产接口保持原行为。测试迁至生产传输 | `tests/test_worker_runtime_protocol.py`、`tests/contracts/test_jobs.py` |
| 失败、取消、恢复及旧任务 | 保留原生回执、停止证明和旧迁移链 | `tests/test_worker_failures.py`、`tests/test_runtime_recovery.py`、`tests/test_process_residency_recovery.py` |
| 任务提交与通知幂等 | 来源、dispatch key、schedule fire 与 Outbox 各保留自身回执 | `tests/test_agent_runtime.py`、`tests/test_schedules.py`、`tests/test_scheduler_supervision.py` |
| QQ 文件与流式上传 | 大小/MIME、lease、去重与取消清理保留 | `tests/test_qq_stream_file_transport.py`、`tests/test_general_execution.py` |
| 邮件与项目 | 当前来源、规则和项目授权在执行时复验 | `tests/test_mail_worker_flow.py`、`tests/test_project_scopes.py` |
| 未挂载的旧 HTTP Worker 路由 | #23 删除后公开地址返回 404 | `tests/test_worker_runtime_protocol.py` 的公开路由检查。源码中保留共享 DTO 与 RuntimeClient |

这些定向套件由现有 CI 的 Python 回归覆盖。对相同提交复用已完成结果。新增契约只需定点检查，相关输入改变后由整合 CI 复验。测试中的模型、QQ 与 OpenViking 使用受控夹具，真实平台凭据与收发验收另行记录。

#23 的依赖参数与测试传输可以作为一个切片回退，数据 schema 与公开管理 API 沿用原契约。历史迁移、旧归档、变更后的真实外部动作及其回执继续按[升级与回退清单](runbooks/releases.md#升级与回退清单)核对。

## 动态准入与进程登记

Scheduler 领取可执行任务后，Worker 仍须调用 `TaskService.bind_process` 登记唯一进程与 session writer。#18 的十任务测量发现该登记入口还继承旧的默认四进程上限，导致已经领取的后续任务失败。默认 TaskService 现按动态准入创建进程，模型请求继续使用 Provider 的容量等待，空闲进程由 SessionPool 的缓存数量与 TTL 回收。独立集成显式传入 `max_resident` 时仍按其上限计数，未证明停止的进程保留占位。

对应回归在 `tests/test_worker_admission.py` 实际执行十次 claim → bind，并保留十个 BUSY 登记。`tests/test_process_residency_recovery.py` 继续核验显式上限、停止证明与原 session writer。进程身份、fence、独占 writer 和停止证明仍在原事务中检查。

## 内部扩展边界

| 扩展面 | 当前入口 | 新实现需通过的检查 |
| --- | --- | --- |
| Provider | `providers.ProviderAdapter`、`ModelCapacity` | 授权、容量等待、流式协议、usage 与超限收尾，见 `test_provider_gateway.py`。 |
| Harness | `contracts/harness.HarnessPort`、`resolve_harness`、Pi 适配器 | binding 版本、原生历史、输入回执、停止与恢复，见[Harness 契约](harness-contract.md)和 `test_pi_harness.py`。 |
| Memory | `contracts/memory.MemoryPort` | owner/workspace/audience、来源、同步水位、撤销和恢复屏障，见上面的四边界矩阵。 |
| Skills | `skills/`、`adapters/hermes_skills/` | 路径与写入授权、技能版本、复盘用途隔离，见 `test_hermes_skills.py`、`test_learned_skills.py`。 |
| Channel | `adapters/qq_official/` 与消息 DTO | 入站身份、目标及平台能力、上传边界、去重与投递未知态，见 `tests/contracts/test_adapters.py`、`test_qq_stream_file_transport.py`。 |
| Tool | `capabilities.types.Capability`、`Action`、ToolGateway | 参数 Schema、动作与资源分类、宿主授权、超时、操作账本与核验，见 `test_capabilities.py`、`test_capability_discovery.py`。 |
| Execution | `contracts/execution.py`、`adapters/execution/` | world binding、workspace、进程树取消、Job 回执及重启恢复，见 `tests/contracts/test_jobs.py`、`test_context_execution.py`。 |

第三方扩展先以一个完整场景实现并通过对应权限与生命周期测试，再形成公开版本约定。插件模板、发现、安装、版本管理和市场由后续真实需求驱动，当前交付使用这些内部入口。
