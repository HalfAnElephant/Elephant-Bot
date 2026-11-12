# Leader / Workers 验收记录

本文对应[实施计划第 11 章](elephant-agent-implementation-plan.md#11-验收矩阵)。Leader/Workers 已切换并部署到 home；宿主回归、真实模型任务、组件和指定恢复场景已验证。完整 QQ 平台验收尚未完成，不能把本记录理解为全部 T/L 场景均已通过。

下方 T/L 表保留各场景的原始覆盖边界；部署与 CI 的最终证据优先见本节。

## 证据边界

| 验证渠道 | 本稿采用的含义 |
| --- | --- |
| 离线定点 | 使用真实领域服务、SQLite、文件或锁定上游工具，外部模型、QQ、systemd 或网络按用例替换为 fixture/mock。只能证明对应宿主契约。 |
| 真实 Pi＋模型桩 | 启动锁定的 Pi 1.5.0 进程与真实扩展，通过确定性 HTTP/SSE 模型桩驱动，验证协议、工具、回执及压缩。模型判断和供应商可用性仍待实测。 |
| home 真实组件 | 在 home 调用实际 systemd user-manager、bubblewrap 或 OpenViking 服务，记录组件行为。单独列出与最终 daemon 的集成范围。 |
| 待统一 CI | 测试存在或有分批定点结果，当前提交的完整 CI 结论仍待补。失败、跳过及未执行的场景均保留待验收状态。 |

home 已使用真实 Pi 1.5.0、deepseek-v4.1-flash 和宿主工具执行任务。官方 adapter 与已验证 QQ owner 绑定均为 0；缺少 AppID、secret 与 OpenID，当前服务没有 QQ 接入。未向旧 NapCat 恢复兼容链路，也未伪造官方身份。

## 已有验证记录

以下结果可复用。主线程应在最终版本补入对应提交、CI URL 和持久化日志位置。

| 记录 | 已执行范围 | 证据入口与限制 |
| --- | --- | --- |
| 领域与恢复定点 | runtime、Leader Core、恢复用例的一批 53 项通过。T10 明确契约修正后另有 8 项记忆重规划定点通过。 | [任务树与会话测试](../tests/test_agent_runtime.py)、[Core 测试](../tests/test_leader_core.py)、[恢复测试](../tests/test_runtime_recovery.py)。包括 fixture 停止证明，真实 root/systemd 停止证明待 daemon 实测。 |
| 原生技能定点 | Hermes adapter 21 项通过，包含真实技能目录、patch、锁、ledger 回退、路径边界与复盘来源。 | [Hermes 测试](../tests/test_hermes_skills.py)。条件复盘调度另见任务树与会话测试，真实模型自主学习仍待验收。 |
| Job 契约定点 | 36 项 Job 契约和 5 项取消回归均有成功结果。并行接口变化影响的 3 项 fixture 已修正并单独复验。 | [Job 契约](../tests/contracts/test_jobs.py)、[任务控制](../tests/test_tasks_control.py)。systemd 调用的离线替身与下述 home 实测分别记录。 |
| Pi 与网关定点 | Pi bridge 加 3 项 admin 检查的一批 20 项通过，包含 1M window、原生压缩、review 工具白名单和偏好刷新。能力发现与 stream cleanup 的一批 23 项通过，包含真实 Pi 的 search→invoke。 | [Pi bridge](../tests/test_pi_bridge.py)、[能力发现](../tests/test_capability_discovery.py)、[admin](../tests/integration/test_admin_operations.py)、[流式交付](../tests/test_qq_stream_delivery.py)。使用模型桩与 QQ HTTP mock。 |
| 故障与接入定点 | Worker/provider/general 相关批次 19 项成功，1 项 fixture 断言修正后独立复验成功。Core/admin/status 相关批次 29 项成功，3 项 fixture 修正后定点复验成功。 | [Worker 故障](../tests/test_worker_failures.py)、[provider](../tests/test_provider_gateway.py)、[执行流程](../tests/test_general_execution.py)。完整组合仍以统一 CI 为准。 |
| 升级与恢复定点 | main 0010 与实验 0010 的迁移形状有定点成功记录，保留原 revision 并使用 `backup=False`。OpenViking 删除记录恢复与 Session 旋转有定点成功记录。 | [旧库升级](../tests/test_legacy_upgrade.py)、[记忆恢复](../tests/test_memory_restore.py)、[操作集成](../tests/integration/test_operations.py)。真实部署库迁移结果待最终部署记录。 |
| OpenViking 契约定点 | HTTP 契约 29 项通过，恢复分支最终 4 项定点复验通过。 | [OpenViking 测试](../tests/test_openviking.py)。该组使用 HTTP mock。 |
| home Job 组件 | 实际 user-manager/bubblewrap 可用，JobService 对象重建后同一 Job 可回读，exit 0 文件写入、exit 7 失败、进程树取消及 invocation 过滤日志有实测成功记录。 | 主机脚本 `/tmp/elephant-job-live.py`。daemon 真实重启后的 Job 续存及整体恢复仍待主线程补验。 |
| home OpenViking 组件 | 实际 v0.5.4.1 服务完成 16 项检查，使用 api_key 与普通 USER，保留原 VLM/embedding 配置。 | 提炼、来源、索引召回、用户隔离、重启、更正、遗忘、旧源重放屏障及只读恢复核对均有真实结果，详见下文。 |

## T01–T26 对照

“已有证据”仅覆盖该列写明的行为。每项的剩余部分继续保留在“待补验收”列。

| ID | 实现与已有证据 | 待补验收 |
| --- | --- | --- |
| T01 | SessionService 负责 Leader 输入，TaskGraph 持久创建根 Goal 与独立 Workers。离线定点覆盖双 Worker、分派键冲突、一次 finalize，真实 Pi＋模型桩覆盖直接聊天和工具执行。 | 真实模型对“小查询／只解释／多步工作”的路由判断，以及最终 daemon 从接受到根交付的整条路径。 |
| T02 | 分派键与原 Task/Attempt/Session 绑定。`test_dispatch_replay_conflict_and_worker_results_finalize_once`、同 Task detach 的执行用例覆盖相关契约。 | 当前完整 CI、部署数据中的 ATTACHED→DETACHED 身份与工具调用次数核对，迁移后真实 daemon 续接。 |
| T03 | Dialogue 独立 Session、逐 Session 写者、统一槽位与 Job 工作区准入已接线。离线定点覆盖切换对话、输入身份与状态查询不占执行容量。 | 同 Dialogue 双根目标与跨 Dialogue 满载混合运行、真实请求配额及控制延迟统计。 |
| T04 | TaskGraph 记录事实进度并节流，LeaderInbox 合并后安排短 progress 轮次，进度完成与根完成分开。 | 大量同阶段事件的专项验证、中文公开摘要质量及真实 QQ 节流效果。当前不能据实现存在认定本项全部通过。 |
| T05 | 已有任务状态与歧义选择入口，进度按阶段合并，终态使旧进度失效。执行、上下文及 QQ workflow 用例覆盖部分行为。 | 自然语言指代、多任务歧义、持续刷屏场景的组合回归及真实模型判断。 |
| T06 | 事件绑定 principal、conversation、owner、Dialogue。离线定点拒绝异主体重放和 question/progress 伪造用户修订，按钮契约包含范围、期限与单次使用。 | QQ 官方不同 AppID、群成员和过期按钮的真实平台验收，相关完整 CI。 |
| T07 | 取消撤销权限并等待 Pi 停止与 Job quiescence，迟到事实按原操作核对。离线恢复/Job 用例和 home Job 进程树取消有成功记录。 | 最终 daemon 宽限停止、真实 root cgroup 证明、旧 generation 与迟到外部回执的组合实测。 |
| T08 | 持久 wait、原生回执、目标修订与终态 CAS 已接线。定点覆盖输入消费、旧 finalizer 拒绝、原等待恢复，真实 Pi＋模型桩覆盖等待与写入同批时阻止写入。 | 满额父子任务、wait(any) 全部收尾及多个早到结果竞态的完整组合验收。 |
| T09 | 源游标、accepted/extracted/indexed 分离与转述去重有 HTTP 契约成功记录。home 真实提炼、索引召回、来源与服务重启召回已验证。 | 最终 daemon 从 Pi 原始导出到跨新 Dialogue 召回的全链路，真实 Worker→Leader→QQ 转述链。 |
| T10 | home 真实更正、遗忘、旧源重放与重启不复活通过。活动根目标登记屏障，等待旧 Worker 停止、Job 清空及操作确定后，以新 Session、新 epoch 和真实用户输入同树重规划。8 项定点覆盖旧结果/goal/制品过滤、linked rework、投递失效及原业务源撤销后 BLOCKED。复用 plan_turn 从保留的真实用户源重建明确产物与检查，缺失要求的空成果 finalize 被拒绝。 | 最终 daemon 在真实 Worker、真实模型运行中更正/遗忘，并验证流式缓存与 queued delivery 的边界。 |
| T11 | Core 在记忆后端异常时保留输入并降级，宿主只保存同步引用、回执与屏障。离线断网、同步恢复及无本地事实体的检查已有成功记录。 | 最终 daemon 中实际停止 OpenViking 时聊天、工具、同步待办与恢复的端到端验收。 |
| T12 | USER、私有/项目 scope、群禁用个人记忆与旧备份屏障有契约覆盖。home 跨 USER、伪造身份 header、只读恢复核对均通过。恢复先导入最新 epoch/屏障及来源回执，保持 RESTORE_REQUIRED 直至核对完成。 | 真实部署库的旧备份恢复演练、项目/群入口及 root 活动任务的整体恢复。 |
| T13 | 锁定 Hermes 原生 schema、SKILL.md、共享 owner 根目录与即时写入。原生文件定点验证创建、发现、读取与跨调用复用，Pi review 只暴露 3 个 Hermes 工具有成功记录。 | 真实模型在最终 daemon 中创建技能，再由新会话与 Worker 发现、复用。 |
| T14 | Hermes patch、支持文件、目录约束、批量失败回退和失败回执有原生工具定点成功记录。 | 最终部署目录权限及真实模型更新既有技能的链路。 |
| T15 | 计数归执行 Session，学习事件去重，复盘有独立 Session 与共享预算，后台 review 来源、read-before-write、禁止递归及不改任务结果有定点成功记录。 | 最终 daemon 中条件自然触发、无可存内容、真实模型复盘和不占 Leader 写者的实测。 |
| T16 | provider/gateway 契约覆盖请求边界、usage、流式中断、代际授权与配额。真实 Pi＋模型桩覆盖工具、压缩与能力发现。OpenViking 提炼另有受限队列。 | 当前完整 CI，真实供应商 429/断流/取消/usage，与冻结旧 Task 运行组合的替换验证。 |
| T17 | Pi 独占原生压缩，已删除旧 history seed。真实 Pi＋模型桩覆盖 1M window、rebind 前压缩、上下文溢出恢复及偏好刷新。恢复读取原生日志与边界回执。 | 完整工具配对、更正、待办与内容引用在最终 daemon 压缩/重启后的组合验收。 |
| T18 | GeneralRun 与 Job 共用执行工作区解析，缺少持久 Job 能力时失败关闭。离线契约及 home systemd/bwrap 的 cwd、文件、退出和取消行为已有证据。 | Pi 与持久 Job 在最终 daemon 的同环境联动，以及控制进程真实重启后续存。 |
| T19 | GitHub 真实目标解析与授权覆盖 flags/URL 顺序、repo、编号、host 和未知形式。恢复复用已有 GET verifier，定点覆盖只读核对与 PARTIAL。 | [GitHub 目标回归](../tests/test_github_verification.py)及[能力回归](../tests/test_capabilities.py)的当前完整 CI，授权测试仓库的真实读写核对。 |
| T20 | 官方四种 surface、媒体、按钮、菜单、关系、流式与 Outbox 路径保留。Pi＋模型桩与 QQ HTTP mock 覆盖部分流式、额度、UNKNOWN 和不重复交付。 | 四种 surface 的真实收发、上传、按钮、菜单、C2C 流式、回复过期和主动拒收。缺少官方凭据与身份绑定，保持待验收。 |
| T21 | LoopSupervisor 提供有界重启与 readiness 故障状态，[监督测试](../tests/test_m0_supervision.py)覆盖异常、停滞及数据库故障。 | 当前完整 CI，最终 daemon 注入 scheduler/delivery 异常后的恢复与健康端点实测。 |
| T22 | OneBot adapter、启动、WebSocket 路由和旧 Worker 监听已退出新路径，旧输入/任务拒绝执行，缺失 adapter 的待发消息转终态。相关离线回归已有成功记录。 | 最终部署后的进程、监听端口与超过 50 条旧待发记录不会阻塞官方队列的统一回归。 |
| T23 | 旧 main/实验 0010 形状有升级定点，保留已发布 revision。迁移可显式关闭备份，恢复保留最新屏障并旋转 Session。 | 实际 home 库迁移、冻结/排空、输入水位和 UNKNOWN 发送保留，需与最终部署日志核对。 |
| T24 | dispatch-intent、immutable key、UNKNOWN/迟到回读和幂等均有离线契约与恢复定点。home Job 对象重建按原 unit 回读，没有二次启动。 | 实际外部请求丢回执、daemon 崩溃和恢复只读核对的组合实测。 |
| T25 | Leader 短轮次与 Goal 独立，原用户输入才能 submit/revise，分派幂等。双 Worker finalize 与非用户 question 伪造修订有定点成功记录。 | 同 Dialogue 多根 Goal、Worker/progress/question/finalize 禁止 submit 的完整 CI，以及真实模型委派后 Leader 继续对话。 |
| T26 | 结果和 Inbox 同事务，persisted/handled 分离，恢复原生边界后结算原 run，稳定通知 ID 避免重复。离线 Leader/Worker 恢复与 UNKNOWN 流式用例已有证据。 | 最终 daemon 在正文持久化前后、Inbox handled 前后崩溃的整组故障注入，以及真实 QQ 最终逻辑通知核对。 |

T10 的明确契约修复覆盖原始用户文本和已认证的后续输入。host API 的独立结构化 requested_outputs/success_checks 当前落在 GoalSpec 与 dispatch_spec，入口传入的空 context 使这类根目标保持在记忆重规划范围之外。扩展该范围时，需要补齐可核对的原始结构化请求来源。

## L01–L14 竞态与恢复对照

| ID | 已有实现与验证范围 | 剩余场景 |
| --- | --- | --- |
| L01 | 离线根目标分派后 Leader 轮次可以完成，Workers 独立。执行测试存在真实 Pi＋模型桩的 12 秒慢工具同 run detach 场景。 | 计划中的持续两分钟场景及真实 daemon 交付全链路。 |
| L02 | 双 Worker、独立根/Session 的领域实现及分派定点已落地。 | 同 Dialogue 两根未完成目标同时运行，一根失败不连带另一根的专项组合。 |
| L03 | `test_input_binding_and_session_writer_survive_dialogue_switch` 验证 Dialogue/Session 与事件重投绑定，结果按 root coordinator 投 Inbox。 | 切换焦点时旧结果投递到原 Dialogue 的完整组合，以及真实 QQ 呈现。 |
| L04 | 分派 key 相同返回原 Task，不同意图拒绝。恢复保留原 run/Session/操作账本，定点结算不会重跑 Worker。 | 精确“提交后、工具回执前”切断进程的真实 daemon 演练。 |
| L05 | 结果/Inbox 稳定 ID、原生 persisted/consumed/finished 回执、停后恢复均有离线定点。 | 注入前、正文落盘后、游标提交前三个切点的逐一组合记录。 |
| L06 | 阶段节流、旧进度 supersede 与过期/UNKNOWN 投递路径已实现，部分流式清理定点通过。 | 大量同阶段事件、终态、回复窗口过期和新触发混合的专项记录。 |
| L07 | 已接受修订形成写屏障，旧版本 finalizer 拒绝，新输入在收尾前消费的定点通过。 | 最后一次真实模型请求在途时注入“只检查，不修改”的完整时序。 |
| L08 | 取消与迟到结果/事实分别结算，Job quiescence 进入取消条件。定点覆盖取消后旧 finalizer 和迟到 operation 不复活任务。 | 真实网络外部写入回执与取消同时发生的演练。 |
| L09 | Leader 停止证明、原生终态恢复和 host failure 保留原输入有定点，恢复不创建替代 Worker。 | 最终 daemon 杀死 Leader，Worker 随后完成，核对恢复只执行必要汇报。 |
| L10 | 统一 max_active、逐模型请求预约、确定状态控制与独立 OpenViking 队列已接线，部分容量与 provider 定点通过。 | 满额 Leader/Worker/review/压缩混合负载，真实请求统计及状态/控制 P95。 |
| L11 | durable wait、让出回执与新旧写资格检查有实现，等待恢复定点和 Pi 混合 wait/write 阻断通过。 | 所有槽位被父任务占用时孩子准入、早到重复结果、wait(any) 的整组验证。 |
| L12 | 工具和修订依赖宿主身份，Worker 结果按资料处理。异主体重放与后台 question 伪造 owner 答复定点通过。 | Worker 结果内嵌伪指令、跨根取消与不同 QQ 主体的完整组合及平台实测。 |
| L13 | 工作区互斥、活跃/UNKNOWN Job 保留目录资格，取消后等待真正停止。Job 契约与 home 进程树取消有证据。 | 两个真实 Worker 竞争同一路径，一方崩溃/取消后的所有权转移。 |
| L14 | 最终通知稳定 ID，过时进度终态化。流式 mock 定点覆盖快完成、UNKNOWN 不 fallback/重发、重启保持 next index。 | 真实 QQ 快完成竞态与最终请求回执丢失。 |

## OpenViking 真实组件验收

服务使用 `v0.5.4.1`，api_key 模式下普通 USER 身份完成数据操作。个人记忆类型为 profile、preferences、entities、events，working_memory、agent_evolution 和自动 commit 关闭。宿主增量同步负责提交与水位。

独立 `elephant-acceptance` account 的 16 项检查均有成功记录，覆盖认证、accepted 与 indexed 分离、真实提炼、来源、索引召回、跨 USER 与伪造 header 隔离、宿主数据库只存引用/回执、服务重启召回、更正、旧源重放不撤销更正、遗忘、重放不复活、重启不复活，以及恢复屏障的只读核对与旧源阻断。

真实服务兼容修正将 batch-write 的 root 从 memories 根改为目标文档所在目录，并使用 vectors_only。更正同时核对内容和索引回执。RESTORE_REQUIRED 期间禁止恢复旧同步写入，显式 reconcile_restore 只使用 GET 核对远端任务、URI 删除或更正 hash，再恢复 READY。未知或冲突证据继续保持阻断。

服务独立并发配置已只读核对，VLM 为 2，embedding 为 1。安装版本解析出的 session_commit 队列上限为 8，reindex 为 4。最终 daemon 与供应商总配额的组合负载仍待验收。

### 固定质量基准

基准使用 20 条稳定 synthetic 项目事实和 20 个不同原始 SourceEvent，执行 20 条直接查询与 10 条释义查询。查询并发为 2。Fixture、原始 HTTP/任务回执和逐查询文档保留在 home 的独立验收数据中。

| 指标 | 实测结果 |
| --- | --- |
| 提炼后的事实与文档 | 20/20 条事实，20 份文档 |
| top-5 来源召回 | 30/30，100% |
| 首位命中 | 30/30 |
| 首位错召回、漏召回、空结果 | 均为 0 |
| top-5 非目标候选 | 150 个返回候选中有 120 个非目标候选，目标候选 precision 为 20% |
| 跨 USER 直接读取 | 20 次全部 403，泄漏 0 |
| 已遗忘内容回流 | 30 次查询均未返回此前已遗忘的 URI，数量为 0 |
| 单批提炼至索引完成 | 69.93 秒 |
| 查询延迟 | P50 100.87 ms，P95 117.05 ms，最大 122.52 ms |
| 提炼 LLM 用量 | prompt 6522，completion 3049，合计 9571 tokens，含 cached 5504、reasoning 1154 |
| 提炼 embedding 用量 | 774 tokens |
| 30 次查询 embedding 用量 | 569 tokens |
| 额外 vectors_only 索引 | 20 次，后端回执未提供 token 计数，记为未知 |

计分要求同一返回文档包含精确系列标识、唯一项目名、正确值和预期原始 source ID，统一空白及代码分隔符。v1 评分器错误要求系列标识与项目名保持连续拼接，最初误计 0/30。OpenViking 将两者规范为同一文档内的系列归属和项目标题，v2 据此分别核对两部分身份，对同一批原始返回重新计分。Fixture、查询、模型/embedding 配置和后端结果均未改变，v1 评分记录继续保留。

来源归因采用保守的批次并集，每份文档均携带该批全部 20 个 source ID。本轮证明了正确项目、事实与预期来源成员同时命中，逐条 source 的精确归属仍未验证。该固定 synthetic 集达到初始 top-5 90% 门槛，真实个人资料与开放式问题的质量仍需独立评估。

摘要文件为 `/tmp/elephant-memory-benchmark.json`。home volume 对应容器目录 `/app/.openviking/acceptance-package/benchmark/`，保留 `fixture.json`、`state.json`、`queries.jsonl`、`http-observations.jsonl`、`initial-scoring-v1.json` 和 `scored-results-v2.json`。此前 16 项验收状态保留在 `/app/.openviking/acceptance-package/state/run.json`。

## home 最终部署与恢复证据

- 单 `elephant-bot.service` 已启用，旧 core/worker acceptance units 均 disabled/inactive。后台监督包含 ingress、delivery、scheduler、external_waiters 和 memory；ready 为 true。发布 revision 记录在 `/opt/qqbot/REVISION`。
- 实际库从历史实验 0010 升级至 0020，`PRAGMA integrity_check=ok`。按用户要求使用 `backup=False`，本次没有备份原 bot。
- 最终代码回归以 [PR #6 的检查](https://github.com/HalfAnElephant/Elephant-Bot/pull/6/checks)为准，包括 Python、console、Ruff、配置 schema、构建/发布冒烟，以及 Python/TypeScript CodeQL。对应原始 JUnit 和日志保留在每次 CI 的 correctness-results artifact。统一渠道使用 CI，不另跑同范围的本地全量测试。
- 真实文件目标 `T-87ade253-056` 达到 SUCCEEDED。根目标聚合子 artifact，通过受保护下载 API 取得 17 字节 `ELEPHANT_FINAL_OK`，hash 与登记值一致。
- 真实 150 秒持久 Job 目标 `T-fd407f5d-145` 达到 SUCCEEDED。Job `fb89e1624083470ca401d2ddb79d9992` 只派发一次；daemon 正常重启前后 InvocationID `222eed5e321d4eb09798aa888e60b96b` 与 PID 保持一致，就绪恢复 1.77 秒。原 Task/Session 续接，新 attempt 不重启业务 Job；最终 EXITED/0，下载的 8 字节 `SURVIVED` 与 artifact hash 一致。
- 在上述目标 Worker 结果持久化后，对 Leader 所在 daemon 主进程注入 SIGKILL。就绪恢复 4.24 秒，已有 Worker attempts 不增加，最终仍由原根目标汇总同一 artifact。原生回执与 root-owned cgroup 空证明是恢复依据。
- 真实取消目标 `T-95bf4657-dcd` 的 root、child、Job 均为 CANCELLED，运行槽释放。控制持久接受耗时 25.62 ms。并发任务期间每批 30 次宿主状态读取的 P95 为 5.9–14.3 ms；这是 HTTP 宿主控制测量，不含模型/QQ 延迟，也不是全角色峰值压力测试。
- 真实模型通过原生 Hermes 工具创建、读取和更新标准 SKILL.md。主机也通过同一工具修正过验收期间形成的错误结算经验，ledger 保留回退记录。真实条件复盘尚无触发成功记录；其计数、调度、只读工具范围及不可递归由契约测试验证。

该批实测发现并修复了：idle JSON null、Scheduler 失败批次监督、外部等待 observer 遗漏、技能回执字段、子 artifact 聚合、关闭 stdin 的 uvloop 竞态、沙箱内用户 D-Bus 不可见、正常 daemon 停机误记任务失败、宿主终态未唤醒父任务，以及 finalizer 显式引用合法 child 证据被拒。早期失败任务与原回执保留，不将这些失败计作成功。

root 的 `task_outcome` 接单与 turn_finished 后原子结算分开。旧故障期间生成的部分模型正文仍描述当时的待核对状态；恢复保留原始正文，不能仅凭模型叙述判断当前状态。应读取根目标 state、原操作回执和 artifact。新回执明确要求完成最终答复并结束轮次，禁止用本轮结算前的 status 判断结算卡死。

home 持久证据：

- `/var/lib/qqbot/leader-live-acceptance.json`：task/run/Job、延迟、正常重启、Leader SIGKILL、artifact 下载校验。
- `/var/lib/qqbot/leader-deployment-facts.json`：revision、迁移、units、监督、学习会话及监听快照。
- `/var/lib/qqbot-daemon-stops/`：root-owned、daemon 只读的旧 epoch 停止证明。
- OpenViking 基准与删除/恢复证据仍位于前述独立 acceptance-package，包含原始评分记录。

## 尚未验收的边界

1. QQ 官方四 surface 的真实收发、窗口、文件、按钮/问题、流式及回执丢失。缺少应用凭据和已验证主体，不能从本地契约或 admin API 成功推断平台通过。
2. 所有 L01–L14 故障切点的真实平台组合、全角色满额压力、真实供应商切换/429/断流与所有控制语义的质量基准。本轮 CI 覆盖对应宿主边界，并补充上述指定真实恢复场景，未执行的组合仍保持未验收。
3. 最终 daemon 经真实 QQ 用户输入自动记忆并跨新 Dialogue 召回，以及活动 QQ Worker 更正/遗忘后的完整交付。OpenViking 真实组件的提炼、更正、遗忘、隔离、重启和基准已执行；活动任务 epoch 屏障与重规划有契约测试。
4. 模型自动技能学习已实际发生；自然触发的独立条件复盘、无可存内容及其真实并发效果仍需独立实测。学习成功不等同于业务执行或 QQ 送达成功。

部署成功与完整验收分别记录。当前能证明单 daemon、任务树、真实业务模型/Job、受保护制品交付及列明恢复路径工作；尚不能宣布实施计划第 12.1 节全部通过。
