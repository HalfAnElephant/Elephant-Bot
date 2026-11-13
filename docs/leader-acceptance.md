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

历史验收快照曾使用真实 Pi 1.5.0、deepseek-v4.1-flash 和宿主工具执行任务，当时官方 adapter 与已验证 QQ owner 绑定均为 0，缺少 AppID、secret 与 OpenID。该状态已由后续同步记录更新：2026-11-13 只读检查观察到官方 QQ WebSocket 为 CONNECTED，详见[服务器源码同步记录](#2026-11-13-服务器源码同步)。连接状态尚未验证四种 surface 的真实收发。旧 NapCat 链路没有恢复。

## 已有验证记录

以下分批记录保留为历史覆盖说明。首轮通过全量验证并部署的运行代码为 `31c23902877bb54851572f82e0dfe3893ef52809`；[CI](https://github.com/HalfAnElephant/Elephant-Bot/actions/runs/43272670267) 的 959 项 Python、17 项 JavaScript 测试均通过且无跳过，构建与发布冒烟通过；[CodeQL](https://github.com/HalfAnElephant/Elephant-Bot/actions/runs/43272670357) 通过。表中“当前完整 CI”等早期待补项已由该结果覆盖；补充批次与真实场景范围见末尾记录，当前发布 revision 以主机部署事实文件为准。

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
| home OpenViking 组件 | 前一版适配器对实际 v0.5.4.1 服务完成 16 项检查，使用 api_key 与普通 USER，保留原 VLM/embedding 配置。 | 这些是该次适配的历史结果；当前逐事件提炼改动尚无新的统一 CI 与服务器验收。旧批次基准和限制见下文。 |

## T01–T26 对照

“已有证据”仅覆盖该列写明的行为。每项的剩余部分继续保留在“待补验收”列。

| ID | 实现与已有证据 | 待补验收 |
| --- | --- | --- |
| T01 | SessionService 负责 Leader 输入，TaskGraph 持久创建根 Goal 与独立 Workers。离线定点覆盖双 Worker、分派键冲突、一次 finalize，真实 Pi＋模型桩覆盖直接聊天和工具执行，HOST 普通文件输入已有一次 Worker 与一次 Leader 的完整交付证据。 | QQ 入口的真实模型路由质量，包括小查询、只解释和多步委派。 |
| T02 | 分派键与原 Task/Attempt/Session 绑定。`test_dispatch_replay_conflict_and_worker_results_finalize_once`、同 Task detach 的执行用例覆盖相关契约。 | 当前完整 CI、部署数据中的 ATTACHED→DETACHED 身份与工具调用次数核对，迁移后真实 daemon 续接。 |
| T03 | Dialogue 独立 Session、逐 Session 写者、统一槽位与 Job 工作区准入已接线。离线定点覆盖切换对话、输入身份与状态查询不占执行容量，home 同 Dialogue 双根目标已实际并行且失败隔离，控制延迟另见生产记录。 | 跨 Dialogue 满载混合运行及供应商配额组合。 |
| T04 | TaskGraph 记录事实进度并节流，LeaderInbox 合并后安排短 progress 轮次，进度完成与根完成分开。 | 大量同阶段事件的专项验证、中文公开摘要质量及真实 QQ 节流效果。当前不能据实现存在认定本项全部通过。 |
| T05 | 已有任务状态与歧义选择入口，进度按阶段合并，终态使旧进度失效。执行、上下文及 QQ workflow 用例覆盖部分行为。 | 自然语言指代、多任务歧义、持续刷屏场景的组合回归及真实模型判断。 |
| T06 | 事件绑定 principal、conversation、owner、Dialogue。离线定点拒绝异主体重放和 question/progress 伪造用户修订，按钮契约包含范围、期限与单次使用。 | QQ 官方不同 AppID、群成员和过期按钮的真实平台验收，相关完整 CI。 |
| T07 | 取消撤销权限并等待 Pi 停止与 Job quiescence，迟到事实按原操作核对。离线恢复/Job 用例、home Job 进程树取消、最终 daemon 实际 Pi 取消及 root cgroup 停止证明均有成功记录。 | 旧 generation 与迟到外部回执的所有真实组合。 |
| T08 | 持久 wait、原生回执、目标修订与终态 CAS 已接线。定点覆盖输入消费、旧 finalizer 拒绝、原等待恢复，真实 Pi＋模型桩覆盖等待与写入同批时阻止写入。 | 满额父子任务、wait(any) 全部收尾及多个早到结果竞态的完整组合验收。 |
| T09 | 源游标、accepted/extracted/indexed 分离、转述去重有契约覆盖。home 实际 HOST Leader 原生源自动提炼/索引，新 Dialogue/Pi Session 召回精确项目值和唯一原始 source ID。 | 真实 Worker→Leader→QQ 三次转述链尚无平台验收。 |
| T10 | home 真实更正、遗忘、旧源重放与重启不复活通过。活动根目标登记屏障，等待旧 Worker 停止、Job 清空及操作确定后，以新 Session、新 epoch 和真实用户输入同树重规划。8 项定点覆盖旧结果/goal/制品过滤、linked rework、投递失效及原业务源撤销后 BLOCKED。复用 plan_turn 从保留的真实用户源重建明确产物与检查，缺失要求的空成果 finalize 被拒绝。HOST 活动 Job 更正后同根关联返工、artifactB 下载，以及真实遗忘确认恢复和活跃循环无复活均已验证。 | QQ 流式缓存与 queued delivery 的真实平台边界。 |
| T11 | home 实际停止 OpenViking，daemon 输入保持 HANDLED/FINISHED 且 skills_list 成功；同一 QUEUED/ConnectError 同步操作在后端恢复后 INDEXED，新 Dialogue 正确回答。无本地备用事实库的宿主检查有定点证据。 | QQ 入口离线组合未验收。 |
| T12 | USER、私有/项目 scope、群禁用个人记忆与旧备份屏障有契约覆盖。home 跨 USER、伪造身份 header、只读恢复核对均通过。恢复先导入最新 epoch/屏障及来源回执，保持 RESTORE_REQUIRED 直至核对完成。 | 真实部署库的旧备份恢复演练、项目/群入口及 root 活动任务的整体恢复。 |
| T13 | 锁定 Hermes 原生 schema、SKILL.md、共享 owner 根目录与即时写入。原生文件定点验证创建、发现、读取与跨调用复用，Pi review 只暴露 3 个 Hermes 工具有成功记录。 | 真实模型在最终 daemon 中创建技能，再由新会话与 Worker 发现、复用。 |
| T14 | Hermes 原生 patch、支持文件、目录/owner 限制及批量失败回退有定点。生产模型与宿主实际通过原生工具更新标准 SKILL.md，目录写入和 ledger 已核对。 | 未扩展额外真实批量故障组合。 |
| T15 | 实际 Agent 计数、事件去重、独立复盘 Session、共享预算、read-before-write、禁止递归和无可存内容均有定点。home 默认阈值 10 自然触发两次真实复盘并 FINISHED，来源结果与原生摘要一致，复盘无业务操作和递归记录。 | 全角色真实峰值混合负载未执行。 |
| T16 | provider/gateway 契约覆盖请求边界、usage、流式中断、代际授权与配额。真实 Pi＋模型桩覆盖工具、压缩与能力发现。OpenViking 提炼另有受限队列。 | 当前完整 CI，真实供应商 429/断流/取消/usage，与冻结旧 Task 运行组合的替换验证。 |
| T17 | Pi 独占原生压缩，已删除旧 history seed。真实 Pi＋模型桩覆盖 1M window、rebind 前压缩、上下文溢出恢复及偏好刷新。恢复读取原生日志与边界回执。 | 完整工具配对、更正、待办与内容引用在最终 daemon 压缩/重启后的组合验收。 |
| T18 | GeneralRun 与 Job 共用执行工作区解析；实际 daemon 中 Pi 创建目录/文件、systemd/bwrap Job 写入、退出/取消与同 Job 跨重启续存已有证据。无持久 Job 能力时失败关闭有契约覆盖。 | 额外执行环境实现仅保留契约替换边界。 |
| T19 | GitHub 真实目标解析与授权覆盖 flags/URL 顺序、repo、编号、host 和未知形式。恢复复用已有 GET verifier，定点覆盖只读核对与 PARTIAL。 | [GitHub 目标回归](../tests/test_github_verification.py)及[能力回归](../tests/test_capabilities.py)的当前完整 CI，授权测试仓库的真实读写核对。 |
| T20 | 官方四种 surface、媒体、按钮、菜单、关系、流式与 Outbox 路径保留。Pi＋模型桩与 QQ HTTP mock 覆盖部分流式、额度、UNKNOWN 和不重复交付。 | 后续只读检查观察到 WebSocket CONNECTED；四种 surface 的真实收发、上传、按钮、菜单、C2C 流式、回复过期和主动拒收仍待验收，连接状态不构成平台通过证据。 |
| T21 | LoopSupervisor 提供有界重启与 readiness 故障状态，[监督测试](../tests/test_m0_supervision.py)覆盖异常、停滞及数据库故障。 | 当前完整 CI，最终 daemon 注入 scheduler/delivery 异常后的恢复与健康端点实测。 |
| T22 | OneBot adapter、启动和 WebSocket 路由、旧 Worker 监听已退出运行；旧输入/任务拒绝执行，缺失 adapter 待发消息终态化。超过 50 条旧队列回归通过，home 旧 units disabled/inactive，进程/监听快照已存。 | QQ 官方队列的真实平台流量未验收。 |
| T23 | main/实验 0010 升级定点与实际 home 库迁移均通过，发布 revision 未改写。实际库达到 0020/integrity ok，按要求 backup=False。旧 backup 恢复时屏障导入、来源回执、Session 旋转与 UNKNOWN 保留有定点。 | 生产旧备份完整灾难恢复演练未执行。 |
| T24 | dispatch-intent、immutable key、UNKNOWN/迟到回读和幂等均有离线契约与恢复定点。home Job 对象重建按原 unit 回读，没有二次启动。 | 实际外部请求丢回执、daemon 崩溃和恢复只读核对的组合实测。 |
| T25 | Leader 短轮次与 Goal 独立，原用户输入才能 submit/revise，分派幂等。双 Worker finalize 与非用户 question 伪造修订有定点成功记录。 | 同 Dialogue 多根 Goal、Worker/progress/question/finalize 禁止 submit 的完整 CI，以及真实模型委派后 Leader 继续对话。 |
| T26 | 结果和 Inbox 同事务，persisted/handled 分离，恢复原生边界后结算原 run，稳定通知 ID 避免重复。离线 Leader/Worker 恢复与 UNKNOWN 流式用例已有证据。 | 最终 daemon 在正文持久化前后、Inbox handled 前后崩溃的整组故障注入，以及真实 QQ 最终逻辑通知核对。 |

T10 的明确契约修复覆盖原始用户文本和已认证的后续输入。host API 的独立结构化 requested_outputs/success_checks 当前落在 GoalSpec 与 dispatch_spec，入口传入的空 context 使这类根目标保持在记忆重规划范围之外。扩展该范围时，需要补齐可核对的原始结构化请求来源。

## L01–L14 竞态与恢复对照

| ID | 已有实现与验证范围 | 剩余场景 |
| --- | --- | --- |
| L01 | `test_two_minute_worker_survives_leader_turn_and_allows_same_dialogue_reply` 用受控时钟验证两分钟独立 Worker、Leader 结束释放槽及原 Dialogue 继续回答；真实 Pi 的慢工具同 run detach 与 home 150 秒 Job 另有证据。 | QQ 呈现未验收。 |
| L02 | 独立根/Session、单写者与双 Worker 分派有领域回归。home 同 Dialogue 两个 Job 曾同时 RUNNING、工作区独立；一次失败隔离试运行暴露下述占位文件导出问题，保留失败结果并复验修复。 | 修复后真实双目标结局记录见最新宿主验收文件。 |
| L03 | `test_old_worker_result_keeps_its_dialogue_after_focus_switch` 验证切换焦点后旧结果仍投原 Session、另一 Dialogue 写者不受影响、旧输入重投保持绑定且 Dialogue 数不增加。 | QQ 呈现未验收。 |
| L04 | `test_leader_dispatch_recovery.py` 在宿主提交后丢回执，停止证明前保留写者，证明后通过锁定 Pi 的 SessionManager 补入真实回执。同一原生日志被真实 Pi 续接，模型收到原 Task ID，仅一个根/Worker。同键异意图或缺少已提交意图拒绝恢复。 | 未支持的未配对工具保持显式核对状态。 |
| L05 | `test_pi_input_recovery.py` 覆盖正文落盘前、落盘后/消费标记前及宿主游标前重启。当前记录用宿主 Session、输入清单和正文 hash 识别已写输入；伪造正文 marker 不构成回执。结果/Inbox 去重、原生边界与 handled 分离由任务和恢复回归验证。 | 历史无清单日志只去重可信主输入，附加输入保守重送；原操作幂等键继续生效。 |
| L06 | `test_progress_flood_final_and_new_trigger_keep_one_current_notification` 连发 50 个同阶段事件只形成一个里程碑，progress 不结算根目标，final 使等待触发的旧进度失效，新触发不复活旧消息。 | 真实 QQ 窗口未验收。 |
| L07 | 新定点验证“只检查，不修改”在途修订后旧 Worker 写授权被拒、旧结果仅排队续接、旧 finalizer 不能结算。已消费自己修订的 Worker 可推进，排队兄弟的待消费输入不会耗尽全局执行能力。 | 真实供应商请求在途的组合未执行。 |
| L08 | 取消先于结果及可信迟到事实的恢复定点保留 CANCELLED、允许只读核对原操作且不再 claim。取消树现在等待活跃子进程停止，父任务持续 CANCEL_REQUESTED。 | 真实外部网络写入与取消同时到达未演练。 |
| L09 | 原生 completed boundary 恢复原 run/result/artifact/单 Inbox，宿主失败保留原输入。home 注入 Leader SIGKILL 后既有 Worker attempts 不增加，原目标最终结算。 | QQ 最终送达未验收。 |
| L10 | `test_host_controls.py` 验证满槽状态/取消快路径、歧义列选项、主体/Dialogue/通知绑定及重投不换目标。请求、重试、压缩和 review 预算由现有定点验证；30 次真实队列取消 P95 为 19.08 ms，状态读取 P95 为 8.32 ms，采样结束活跃槽位为 4。 | 该测量为宿主控制，不含模型及 QQ 延迟；全角色持续峰值压力未执行。 |
| L11 | `test_agent_wait_capacity.py` 验证父任务占满全部槽位、未配对/未停不释放、持久让出后孩子准入、早到/重复结果单次唤醒及同 Task/Session 续接。wait(any) 撤销剩余孩子写资格，根目标等剩余进程证停后才可汇总。 | 真实 QQ 展示未验收。 |
| L12 | 定点将恶意 owner 指令放入 Worker 结果后，finalize 仍不能 submit 或取消别的根；Worker 跨根取消被拒。其他成员/另一 AppID 无法答复真实开放问题，宿主通知回复按实际 wait 绑定且 answer 不广播给兄弟。 | QQ 不同账号/应用的真实平台演练未执行。 |
| L13 | 共享工作区 claim 互斥、Job 存活/UNKNOWN 时继续持有目录资格、未证明旧进程停止不能转移写者均有定点。home 并发两个 Job 使用独立工作区写同名路径。 | 真实共享绑定的崩溃/取消组合未执行。 |
| L14 | 新定点使快完成后的 RETRYABLE 派发确认被投递门禁拒绝，最终通知只保留一个；最终 UNKNOWN 经新触发、同结果重投或换措辞不能再发。旧检索正文跨记忆 epoch 被拒，真实宿主记忆更改确认可按精确持久回执投递。 | 真实 QQ 回执丢失未演练。 |

## OpenViking 真实组件验收

服务使用 `v0.5.4.1`，api_key 模式下普通 USER 身份完成数据操作。个人记忆类型为 profile、preferences、entities、events，working_memory、agent_evolution 和自动 commit 关闭。宿主增量同步负责提交与水位。以下 16 项和固定基准是前一版适配器的历史实测；当前逐事件提炼实现的代码验证以对应 PR 的统一 CI 为准，新路径的服务器验收仍待执行。

独立 `elephant-acceptance` account 的 16 项检查均有成功记录，覆盖认证、accepted 与 indexed 分离、真实提炼、来源、索引召回、跨 USER 与伪造 header 隔离、宿主数据库只存引用/回执、服务重启召回、更正、旧源重放不撤销更正、遗忘、重放不复活、重启不复活，以及恢复屏障的只读核对与旧源阻断。

真实服务兼容修正将 batch-write 的 root 从 memories 根改为目标文档所在目录，并使用 vectors_only。更正同时核对内容和索引回执。RESTORE_REQUIRED 期间禁止恢复旧同步写入，显式 reconcile_restore 只使用 GET 核对远端任务、URI 删除或更正 hash，再恢复 READY。未知或冲突证据继续保持阻断。

服务独立并发配置已只读核对，VLM 为 2，embedding 为 1。安装版本解析出的 session_commit 队列上限为 8，reindex 为 4。最终 daemon 与供应商总配额的组合负载仍待验收。

### 固定质量基准（旧批次归因，历史结果）

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

该基准属于旧批次归因实现：每份文档均携带该批全部 20 个 source ID。本轮只证明正确项目、事实与预期来源成员同时命中，未证明逐条 source 精确归属。当前新提炼按原始事件分别提交，每个事件需独立 commit；这会增加远端 commit 次数。30/30 top-5 是旧实现的历史指标，不代表当前逐事件提炼的 source 精确归因或召回质量。旧批次文档无法从历史回执恢复唯一 source ID，显式按 source ID 遗忘会覆盖该来源的全部候选派生文档；对已知 URI 的定向更正/遗忘可保留同批其他来源。新提炼路径尚未补做真实基准。

摘要文件为 `/tmp/elephant-memory-benchmark.json`。home volume 对应容器目录 `/app/.openviking/acceptance-package/benchmark/`，保留 `fixture.json`、`state.json`、`queries.jsonl`、`http-observations.jsonl`、`initial-scoring-v1.json` 和 `scored-results-v2.json`。此前 16 项验收状态保留在 `/app/.openviking/acceptance-package/state/run.json`。

## home 最终部署与恢复证据

- 单 `elephant-bot.service` 已启用，旧 core/worker acceptance units 均 disabled/inactive。后台监督包含 ingress、delivery、scheduler、external_waiters 和 memory；ready 为 true。发布 revision 记录在 `/opt/qqbot/REVISION`。
- 实际库从历史实验 0010 升级至 0020，`PRAGMA integrity_check=ok`。按用户要求使用 `backup=False`，本次没有备份原 bot。
- 最终代码回归以 [PR #6 的检查](https://github.com/HalfAnElephant/Elephant-Bot/pull/6/checks)为准，包括 Python、console、Ruff、配置 schema、构建/发布冒烟，以及 Python/TypeScript CodeQL。对应原始 JUnit 和日志保留在每次 CI 的 correctness-results artifact。统一渠道使用 CI，不另跑同范围的本地全量测试。
- 最终干净模型目标 `T-03be12cc-338` 由一次 Worker 和一次 Leader 汇总达到 SUCCEEDED，产物 `outputs/final-summary.txt` 为 8 字节 `FINAL_OK`，最终正文准确说明路径、内容和核对结果。此前真实文件目标 `T-87ade253-056` 达到 SUCCEEDED。根目标聚合子 artifact，通过受保护下载 API 取得 17 字节 `ELEPHANT_FINAL_OK`，hash 与登记值一致。
- 真实 150 秒持久 Job 目标 `T-fd407f5d-145` 达到 SUCCEEDED。Job `fb89e1624083470ca401d2ddb79d9992` 只派发一次；daemon 正常重启前后 InvocationID `222eed5e321d4eb09798aa888e60b96b` 与 PID 保持一致，就绪恢复 1.77 秒。原 Task/Session 续接，新 attempt 不重启业务 Job；最终 EXITED/0，下载的 8 字节 `SURVIVED` 与 artifact hash 一致。
- 在上述目标 Worker 结果持久化后，对 Leader 所在 daemon 主进程注入 SIGKILL。就绪恢复 4.24 秒，已有 Worker attempts 不增加，最终仍由原根目标汇总同一 artifact。原生回执与 root-owned cgroup 空证明是恢复依据。
- 真实取消目标 `T-95bf4657-dcd` 的 root、child、Job 均为 CANCELLED，运行槽释放。控制持久接受耗时 25.62 ms。并发任务期间每批 30 次宿主状态读取的 P95 为 5.9–14.3 ms；这是 HTTP 宿主控制测量，不含模型/QQ 延迟，也不是全角色峰值压力测试。
- 真实模型通过原生 Hermes 工具创建、读取和更新标准 SKILL.md。主机也通过同一工具修正过验收期间形成的错误结算经验，ledger 保留回退记录。默认阈值 10 下，两次真实条件复盘均在独立 Session 达到 FINISHED：`2b0ea34b-0184-4eda-b242-fbd1848589a3` 与 `98b26cc2-e519-44a2-bc2c-d3f203ed3fe4`。原来源轮次计数均为 11；原生日志中仅有 skills_list、skill_view、skill_manage，工具均配对且无 inflight。未发现复盘归属的业务操作/事件、学习记录或递归复盘，原来源结果与其原生摘要一致。此为事后只读核对，未创建复盘前数据库快照。

该批实测发现并修复了：idle JSON null、Scheduler 失败批次监督、外部等待 observer 遗漏、技能回执字段、子 artifact 聚合、关闭 stdin 的 uvloop 竞态、沙箱内用户 D-Bus 不可见、正常 daemon 停机误记任务失败、宿主终态未唤醒父任务，以及 finalizer 显式引用合法 child 证据被拒。早期失败任务与原回执保留，不将这些失败计作成功。

root 的 `task_outcome` 接单与 turn_finished 后原子结算分开。旧故障期间生成的部分模型正文仍描述当时的待核对状态；恢复保留原始正文，不能仅凭模型叙述判断当前状态。应读取根目标 state、原操作回执和 artifact。新回执明确要求完成最终答复并结束轮次，禁止用本轮结算前的 status 判断结算卡死。

home 持久证据：

- `/var/lib/qqbot/leader-live-acceptance.json`：task/run/Job、延迟、正常重启、Leader SIGKILL、artifact 下载校验。
- `/var/lib/qqbot/leader-deployment-facts.json`：revision、迁移、units、监督、学习会话及监听快照。
- `/var/lib/qqbot-daemon-stops/`：root-owned、daemon 只读的旧 epoch 停止证明。
- `/var/lib/qqbot/acceptance-natural-review/production-review-facts-c6055f8e.json`：默认阈值自然复盘、原生日志 hash、来源结果及工具范围的只读核对。
- OpenViking 基准与删除/恢复证据仍位于前述独立 acceptance-package，包含原始评分记录。

## 补充批次的宿主证据与修复

原始计划 11.3 节允许 fake/replay Harness、工具和受控时钟验证 L01–L14。上表采用这一范围，真实 Pi、模型、Job 和平台结果分别保留，未把所有竞态改成必须在 QQ 平台逐一注入。

实际 daemon 的可信 HOST 入站钩子复用了已认证 admin_api owner 事件，输入经 Core.inbox.accept 后由运行中的 Core.process、Scheduler 和 Pi 处理。原生源自动达到 OpenViking INDEXED，全新 Dialogue/Pi Session 召回精确项目值和唯一原始 source ID。实际停止 OpenViking 后，Leader 仍完成输入及 skills_list，同步留下 QUEUED/ConnectError；后端恢复后同一操作 ID 达到 INDEXED，新 Dialogue 正确回答。8 项断言全部通过，记录在 `/var/lib/qqbot/acceptance-memory-daemon/46f5d4cc-4212-482e-b704-817351ee9f7b/summary.json`。这是 HOST 链路，QQ 平台范围仍按下节记录。

该实测也暴露了“记住偏好”被模型误作写本地文件的行为。单一提示词现已说明自动原生同步和实际记忆回执，不为记忆另建文件/Worker 保存链。错误根目标已取消，原文件和失败回执保留，后续新提示词验证写入最新 HOST 验收事实。

同 Dialogue 的双目标试运行中，失败 Job 按设计退出 7，另一 Job 实际退出 0 并写出文件，但 `outputs/.keep` 被自动当作可交付制品，导致成功分支在导出时失败。正常结算与停止后恢复现均仅自动收集支持的输出文件，显式附件在工具边界校验；TaskSpec 的机器验收检查只接受 artifact、observation、effect，产物必须是可导出的相对文件路径，业务完成标准写在 goal。无效要求在接单事务中拒绝，不再执行到最后才发现不可验收。原失败结果保留在 `/var/lib/qqbot/leader-isolation-acceptance.json`，未计作双目标通过。 Leader 对子任务 Job 的只读 inspect 现由宿主按实际 Job/Task 冻结绑定授权，绑定根、当前租约、主体/会话及读取权限在异步读取后再次核对，Worker 的精确作用域保持原契约。

30 个不同根目标经真实 HTTP 管理入口接单后立即取消，全部 CANCELLED，未派发 Job；唯一抢先启动的 run 已确认停止并释放槽。取消持久接受 P95 19.08 ms，30 次状态读取 P95 8.32 ms，记录在 `/var/lib/qqbot/leader-control-benchmark.json`。本批生产复验的输入、模型结果、任务树和制品校验另存最新 HOST 验收事实文件，发布版本以 `/var/lib/qqbot/leader-deployment-facts.json` 为准。

## 后续真实入口发现的问题

`12b5580` 的统一 CI 为 1039 项 Python、17 项 JavaScript 全部通过且无跳过，构建、发布冒烟和 CodeQL 通过。该版真实双目标中的成功根 `T-b90afba2-654` 已 SUCCEEDED，空的 `outputs/.keep` 留在工作区，唯一导出产物为 11 字节 `ISOLATED_OK`，受保护下载 hash 一致。失败分支的 Job 已退出 7，但 Worker 使用 `job.inspect/data.state` 等待，暴露外部等待器只处理 GitHub/web 的遗漏。当前补丁由同一等待器调用宿主按原 wait、Worker、Job 作用域读取，支持已有 data.state 条件，续接原 Task/Session，不再派发 Job。

新版本的普通 HOST 记忆输入已确认收到并自动 INDEXED，新 Dialogue 回答精确值和原始来源，没有 Worker 或文件替代链。真实模型还修正了一条先前错误学到的本地文件记忆技能。普通文件任务随后在绑定 Pi 进程前失败，原因是旧 daemon 的 IDLE 驻留记录仍占容量。启动回收现在仅按可信旧 epoch 停止证明标记物理进程 STOPPED，活动 run 的槽位与 Session 写者仍由原生恢复处理。无证明和当前 epoch 的记录保留，空 PID 不构成停止证明。

等待 Job 时收到真实任务修订会持久登记受影响 Job 的停止意图，旧等待被替代，同 Task/Session 可读取新输入。旧 Job 尚未证停时，新业务写入和根目标验收继续阻断。问题答复只影响对应 Task，兄弟 Job 可继续运行，停止意图不会因输入已消费而丢失。dispatch 期间权限变化后仍保留真实调用回执并登记停止，避免把已经发生的派发当作未执行。

## 最新运行复验

`db1b4eb` 的统一 CI 为 1070 项 Python、17 项 JavaScript 全部通过且无跳过，Ruff、配置 schema、构建/发布冒烟与 CodeQL 均通过，已部署到 home。旧 IDLE 驻留记录 `889f6490-4375-4209-aaf7-7f6012ea4368` 已凭可信旧 epoch 停止证明成为 STOPPED。

原失败分支 `T-4e232c35-b0b` 的 Job 等待在部署后唤醒原 Task 与同一 Worker Session，最终 BLOCKED，原 Job 仍只有一个且 exit 7。另一根目标保持 SUCCEEDED，已核对的产物保持原 hash。恢复事实写入 `/var/lib/qqbot/leader-isolation-fixed-acceptance.json` 的 post_fix_checks。

真实 HOST 普通文件重试已完成 Leader → Worker → 文件读回 → Leader 核验闭环。根目标 `T-19dd86df-b83` 达到 SUCCEEDED，只有一个 Worker attempt 和一个 finalizer，均 FINISHED、槽位为 0，没有 Job。`outputs/host-final.txt` 为精确 13 字节 `HOST_FINAL_OK`，受保护下载的原始字节及 SHA-256 与登记值相符。该重试保留了先前绑定前失败的目标，记录在 `/var/lib/qqbot/acceptance-host-final/3828bb49-8ecc-4c48-b004-79eedf310331/summary.json`。

真实 Pi 已启动后的取消接受耗时 24.83 ms，但首次探针发现进程退出后任务槽未释放，原失败结果保留在 `/var/lib/qqbot/leader-cancel-process-fixed-acceptance.json`。原因是取消 stdout reader 时，满队列的 EOF 入队等待没有消费者，阻塞停止回执。后续 `47d397e` 的统一 CI 为 1073 项 Python、17 项 JavaScript 全部通过且无跳过，构建/发布冒烟和 CodeQL 通过。部署后，原取消任务凭可信停止证明自然恢复为 CANCELLED。新探针 `T-346170d1-060` 先观察实际 Pi PID 与启动时间，再发取消；接受耗时 33.58 ms，随后根目标 CANCELLED、run FINISHED 且 slot_reserved=0、process STOPPED，原 PID 已停止。新成功证据独立保存在 `/var/lib/qqbot/leader-cancel-process-final-acceptance.json`，首次失败记录保留。

真实 HOST 活动任务更正已完成同根返工。根目标 `T-52013406-6de` 原 Job `897934bd5b0943c2be1a23b7a07e22c5` 处于 RUNNING 时，通过真实 `memory_correct` 更正唯一测试项目，操作达到 APPLIED，owner epoch 从 0 升至 1。旧 child 与 Job 均 CANCELLED，确认原 PID 消失且 cgroup 消失或为空后，系统才建立关联旧任务的 rework。新 Worker 使用新 Session、epoch 1，替代 Job EXITED/0，原根最终 SUCCEEDED。受保护制品下载的 71 字节为更正后的 archive-b 值，hash 与登记一致。中间 Leader 轮次 `cbc941bc-527d-4a71-9280-da548ab24a26` 的 aborted 结果保留；该轮已持久派出关联子任务，后续正常汇总，无额外业务 Job 或最终阻塞。证据仍在上述 HOST summary 和原生日志中。

三文档遗忘已在真实后端达到 APPLIED，epoch 为 2，三份 synthetic 文档均 DELETED 且直接 GET 返回 404。首次宿主确认暴露新的竞态：适配器登记 epoch 屏障后等待远端操作，期间旧轮次被控制检查撤销，尚未写入 mutation marker；旧输入随后因过期上下文被反复领取，共产生 393 个 attempt。后续 392 次均在绑定前失败，未启动 Pi 或模型。daemon 已停止以终止循环，原生调用、失败和删除回执保留。

修复使 correct/forget 仅同步登记并立即返回持久 receipt，由原后台队列执行远端操作。崩溃窗口通过原生 run/输入/工具 ID 与完整参数 hash 精确匹配已提交操作，立即轮换后续对话历史，并在所有相关 attempt 与旧会话写者停止后复用宿主确认。已释放槽位的原失败结果保存在原 receipt 的 recovered_result 中，后续失败 attempt 保持原样。缺少停止证明的已匹配输入继续待核验；无匹配回执的永久失效上下文在分配 run 前终止并记录 CONTEXT_REVOKED。畸形原生日志隔离在该输入的匹配范围，原操作不重放。`6502148` 已通过统一 CI 的 1102 项 Python、17 项 JavaScript 测试，均无跳过，Ruff、配置 schema、构建/发布冒烟与 CodeQL 通过，并已部署。原遗忘输入已由原 run 收口为 HANDLED，原 operation ID 与 epoch 2 保持不变。393 个 attempt ID 均保持，其他 392 条整行 hash 不变；原 `5884e9ca...` 仅 request/result/receipt 三字段变化，旧失败逐字归档，原生日志 hash 不变。CORRECT 1、FORGET 1、SYNC 30 的数量未增加，没有重新遗忘或重注入。新 Session 已隔离旧历史，槽位为 0。

同一 daemon Invocation/PID 持续运行 93.99 秒，memory 循环 last_success 推进，三个 URI 在窗口前后实际 GET 均为 404。完整对照位于上述 HOST 目录的 `forget-recovery-proof.json`，部署前全量基线为 `forget-recovery-baseline.json`。确认的范围是一次宿主逻辑结果和一条 Outbox，`Core.pre_send` 实际返回 true；HOST 缺 channel adapter，因此该 Outbox 的实际状态为 FAILED/ADAPTER_REMOVED，未计作平台送达。

遗忘后的新 Dialogue/Session 普通输入也已实际通过。唯一 Leader run `8daf7444...` 达到 HANDLED/FINISHED、epoch 2、slot 0，答复精确为“恢复正常”，原生工具调用为 0，没有 Root、Worker 或 Job，所有运行槽为 0。源、上下文及原生日志 hash 保存在 HOST summary 的 post_forget_normal_probe。运行代码的统一检查为 [CI 43300742366](https://github.com/HalfAnElephant/Elephant-Bot/actions/runs/43300742366) 与 [CodeQL 43300742204](https://github.com/HalfAnElephant/Elephant-Bot/actions/runs/43300742204)。

## 2026-11-13 服务器源码同步

本次以 home 的 `/opt/qqbot` 为源码依据，同步到本地工作区并更新现有 PR #6。服务器 `REVISION` 仍为 `9b77a1b`，其后直接部署的修改通过逐文件内容比对纳入本次提交，单凭该文件不能识别完整运行源码。

同步内容包括动态 Worker 调度、模型请求背压、活动目标复用与补充、Leader 进度合并、失败升级与受控接手、原生轮次崩溃恢复，以及统一人格文件。服务器配套测试一并纳入，并保留本地已有的扫码 owner 经持久 Outbox 投递的身份与撤权回归。README 补充当前代码入口，部署说明补充兼容字段与执行限制语义。

同步范围为源码、测试、依赖锁、迁移、示例配置和部署文档。运行配置、凭据、数据库、Session 与业务产物保留在各自主机。验证沿用 PR 的统一 CI；此前提交的成功检查仅对应此前版本，本次同步结果以新提交的检查为准。该次同步没有重新执行生产故障注入或 QQ 平台验收。

同步时只读检查显示 daemon 为 active/running，live 与 ready 均返回 HTTP 200，后台监督健康，官方 QQ WebSocket 为 CONNECTED。此前缺少 QQ 凭据或无 adapter 的描述属于前批次历史状态；当前连接状态仍需配合真实平台收发验收。

同步审查发现 Leader 接手的静态时序风险。失败 Worker 已释放运行槽、Job 维护尚未登记取消时，持久 Job 可能仍运行。后续 review 修复将 `TaskGraph.finalize_ready` 的停止屏障覆盖到所有非终态 Job，回归覆盖 INTENT、STARTING、RUNNING 和 UNKNOWN，要求 Job 终止后才能接手。

## 尚未验收的边界

1. QQ 官方四 surface 的真实收发、窗口、文件、按钮/问题、流式及回执丢失。后续[同步记录](#2026-11-13-服务器源码同步)显示 WebSocket CONNECTED；仍需验证各 surface 的实际收发与授权主体，不能从连接状态、本地契约或 admin API 成功推断平台通过。
2. 所有 L01–L14 故障切点的真实平台组合、全角色负载、真实供应商切换/429/断流与所有控制语义的质量基准。部署旧备份恢复演练和最终 daemon 压缩组合也未新增实测。本轮 CI 覆盖对应宿主边界，并补充上述指定真实恢复场景，未执行的组合仍保持未验收。
3. 最终 daemon 经真实 QQ 用户输入自动记忆并跨新 Dialogue 召回，以及活动 QQ Worker 更正/遗忘后的完整交付。OpenViking 真实组件的提炼、更正、遗忘、隔离、重启和基准已执行；活动任务 epoch 屏障与重规划有契约测试。
4. 模型自动技能学习与自然触发的独立条件复盘已实际发生，“无可存内容”分支已有宿主定点验证。全角色真实并发效果仍未执行。

部署成功与完整验收分别记录。当前能证明单 daemon、任务树、真实业务模型/Job、受保护制品交付及列明恢复路径工作；尚不能宣布实施计划第 12.1 节全部通过。


## 2026-11-13 PR review 收口

PR#6 承接 Leader/Workers 主线。PR#4 的 GitHub 授权与效果回读、安全制品、持久续接等价值由当前实现保留，技能使用 Pi/Hermes，长期记忆使用 OpenViking。OneBot、social/Judge、黑话与 research 专用运行链按实施计划退役。

本批修复同 daemon 已证停 Worker 的租约恢复、恢复 IO 阻塞、超出旧执行 deadline 的有效问题答复、网页逐跳授权和共享请求时限，以及旧 owner 字段为空任务的管理可见性。未知进程继续等待停止证明。后台循环持续退避重试并保持健康告警。Git clone 恢复 branch 参数，分支校验使用 Git 原生规则。

制品每小时按既有 30 天下载契约清理，保留活动任务树、占槽 run 与未到期引用。持续存在活动任务时，未入库的孤儿导出延后清理，以保全 spool 恢复证据。

本批统一验证以 PR#6 当前提交的 CI 和 CodeQL 为准。本批未执行生产故障注入和 QQ 客户端验收。
