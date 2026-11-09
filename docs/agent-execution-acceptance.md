# 通用 Agent 执行验收

本表对应方案 v3 的 A01–A62，记录当前源码中的检查入口和证据边界。`FIXTURE` 表示存在确定性测试，`REAL_PI` 表示使用锁定的 pi 1.5.0 与本地模拟模型，`PARTIAL` 表示只覆盖部分条件，`UNVERIFIED` 表示尚缺对应验收。测试存在和测试执行成功分别记录。GitHub 远端写入、QQ 真机交付与目标 Linux 隔离需单独验收。

## 验证证据

- 本地全量首次执行 248 项：244 通过，4 项失败。调度公平性、观察记录测试夹具、持久内容读取断言和失败说明续接预期已修复，失败项均已定点复验通过。
- 主代理最后执行链路回归覆盖 82 项；其中一处同时间戳 fixture 的不确定顺序已修正并定点通过，其余 81 项通过。范围为 general execution、pi bridge、task control、provider gateway 和 execution migration。
- `tests/integration`、`tests/unit`、`tests/test_social_memory.py` 合计 30 项通过；随后新增管理 API 问题送达计时检查及原 UNKNOWN 处理回归共 2 项通过。
- `tests/test_context_execution.py` 最终 44 项通过，覆盖超限输入、话题与引用、权限撤销、独立开关、配置和 pool 参数传递。
- `tests/test_capabilities.py` 60 项通过，随后进程容量调整对应的 9 项定点检查通过。`gh api` 选项顺序/跨 host 校验随后另有 36 项定点通过，保证检查的是实际 endpoint，正文链接保持原样。真实 GitHub 写入尚未执行。
- 真实 pi 1.5.0 使用本地模拟模型，验证 warm 两轮、同 run 后台化、短任务并发、活跃/结束边界补充输入、混合等待批次和自动压缩。压缩阈值用模拟 usage 触发，不代表真实供应商计费或性能。
- 0005→0007 迁移测试验证私有备份、旧任务/保留槽不丢失及再次迁移幂等。二进制文件经过 Worker 导出和 Core 校验；artifact 边界 11 项通过。
- 本次采用本地验证，未运行云端 CI、生产部署或真实外部写入。Ruff 范围为源码、测试及新增迁移；另检查安装脚本语法、runner entry 编译和 diff 空白。

## 验收矩阵

| ID | 状态 | 检查入口与仍需确认的范围 |
| --- | --- | --- |
| A01 | REAL_PI | `test_general_execution.py::test_locked_pi_warm_rebind_no_extra_model_call` 每次问候一个模型请求，模型为本地 fixture。 |
| A02 | UNVERIFIED | 公网抓取沿用 `test_pi_research.py::test_ssrf_blocked_and_redirect_schemes` 的边界检查，嘉定实时天气的端到端结果尚未验收。 |
| A03 | PARTIAL | `test_capabilities.py::test_empty_pr_list_is_successful_without_files` 与 `test_cli_literal_stdin_and_bounded_output` 覆盖空结果和截断，真实 PR 分页尚未验收。 |
| A04 | PARTIAL | `test_general_execution.py::test_no_file_result_detach_and_protocol` 允许无文件完成，长时间故障调查尚未端到端运行。 |
| A05 | FIXTURE | `test_general_execution.py::test_requested_output_still_required` 拒绝缺失指定文件；`test_worker_exports_requested_binary_without_text_write_tool` 验证命令生成的二进制经 Worker/Core 原样导出。Office MIME 已登记，实际办公文档渲染未验收。 |
| A06 | FIXTURE | `test_capabilities.py::test_discovery_describes_and_invokes_new_capability` 经发现、描述和通用调用接入新能力。 |
| A07 | REAL_PI | `test_general_execution.py::test_twelve_second_tool_detaches_same_run_and_short_chat_starts` 实际等待 5.1 秒后 detach，12 秒工具只执行一次，Task/run/PID/session 保持不变；时钟超时由 fixture 推进。 |
| A08 | FIXTURE | `test_general_execution.py::test_no_file_result_detach_and_protocol` 排队 detach 时不存在伪 run。 |
| A09 | PARTIAL | 同一 12 秒工具测试确认长任务未完成时新短交互在 4 秒内完成。容量有界；最大容量饱和、多会话压力和真实模型延迟仍需部署验收。 |
| A10 | REAL_PI | `test_general_execution.py::test_locked_pi_warm_rebind_no_extra_model_call` 连续两轮复用同 PID 并轮换 token。 |
| A11 | PARTIAL | `test_context_execution.py::test_context_delivered_assistant_scope_and_permission_revocation` 拒绝跨 conversation 来源，真实多群 transcript 隔离尚未验收。 |
| A12 | REAL_PI | `test_general_execution.py::test_locked_pi_warm_rebind_no_extra_model_call` 验证 quiescent、IDLE 和空 runner_stopped_at。 |
| A13 | PARTIAL | `test_general_execution.py::test_root_pi_marker_does_not_prove_host_tools_stopped` 拒绝只凭 pi marker 释放槽；pool 故障测试确认停止后冷启动。目标 Linux Worker 崩溃/cgroup 恢复尚未验收。 |
| A14 | REAL_PI | `test_pi_bridge.py::test_locked_pi_rebind_precedes_auto_compaction_and_post_request_passes_gate` 确认新 binding/token 先于压缩；`test_provider_gateway.py::test_binding_authorization_pins_generation` 阻止旧 binding 请求跨等待取得新凭据。 |
| A15 | PARTIAL | session adapter 拒绝 busy run，Core 保存进程绑定，缺少同 session 并发 CAS 的专门竞争测试。 |
| A16 | FIXTURE | `test_permissions.py::test_default_allows_write_and_management`、`test_adapter_default_ignores_legacy_allowlists_but_keeps_platform_checks` 覆盖默认读写和旧白名单。 |
| A17 | FIXTURE | `test_permissions.py::test_subject_specificity_priority_and_namespace` 与 `test_persist_cas_audit_and_immediate_revocation` 覆盖主体和撤权。 |
| A18 | FIXTURE | `test_permissions.py::test_resource_action_specificity_and_deny_tie`、`test_persist_cas_audit_and_immediate_revocation` 检查优先级与热更新。 |
| A19 | FIXTURE | `test_provider_gateway.py::test_forged_tool_identity_is_ignored_by_host`、`test_malformed_bridge_packet_rejected` 使用宿主 task/run/fence/principal。 |
| A20 | PARTIAL | `test_context_execution.py::test_context_delivered_assistant_scope_and_permission_revocation` 检查默认来源边界，显式跨 scope 操作尚缺完整端到端用例。 |
| A21 | PARTIAL | `test_capabilities.py::test_review_readback_requires_submitted_identity_commit` 核验 review ID/state/commit；`test_commit_bound_review_transmits_literal_json` 检查 commit_id。真实提交尚未执行。 |
| A22 | PARTIAL | `test_provider_gateway.py::test_control_response_cannot_execute_business_tools` 检查受限目标，模型仅查看 PR 时不自行 approve 的语义验收尚未执行。 |
| A23 | FIXTURE | `test_capabilities.py::test_classification` 覆盖字段隐式 POST、input body、显式 GET、GraphQL query/mutation 和 REST review event。复杂 GraphQL mutation 保守记 unknown。 |
| A24 | PARTIAL | `test_capabilities.py::test_classification`、`test_permissions.py::test_unknown_action_restrictions_are_subject_scoped` 检查未知命令与限制。真实 alias/extension 和全部旁路组合尚未验收。 |
| A25 | FIXTURE | `test_capabilities.py::test_cli_literal_stdin_and_bounded_output`、`test_commit_bound_review_transmits_literal_json` 检查中文、换行、引号、反引号与美元符号。 |
| A26 | FIXTURE | `test_capabilities.py::test_review_readback_requires_submitted_identity_commit` 拒绝把 PENDING 当已提交。 |
| A27 | FIXTURE | 同一 review 回读测试记录 reviewed/current SHA 并识别 head_changed；commit-bound review 测试检查 REST 精确提交参数。 |
| A28 | PARTIAL | `test_capabilities.py::test_merge_auto_is_distinct_from_merged` 区分 auto merge 与 merged。Merge queue 的确定回读目前保留 UNKNOWN。 |
| A29 | PARTIAL | capability 的同 ID/语义去重测试覆盖 PREPARED/DISPATCHED/UNKNOWN/PARTIAL/VERIFIED；`test_general_execution.py::test_reconciled_write_is_unknown_and_retry_uses_original_ledger` 验证停止后 DISPATCHED→UNKNOWN、未派发 PREPARED→FAILED，以及跨 retry 保留旧操作。真实断网/重启回读未演练。 |
| A30 | FIXTURE | `test_capabilities.py::test_create_nonzero_with_remote_object_is_partial` 保存已创建对象并返回 PARTIAL。 |
| A31 | PARTIAL | `test_tasks_control.py::test_no_artifact_success_and_notification_identity` 检查通知 ID 与重放，长任务多次真实中途反馈尚未验收。 |
| A32 | FIXTURE | 同一通知测试检查 accepted 和 notification_id；平台送达由 Outbox 单独记账。 |
| A33 | PARTIAL | `test_tasks_control.py::test_no_artifact_success_and_notification_identity` 检查同 ID 正文冲突，实际网络发送重试的模型调用计数尚缺专门检查。 |
| A34 | REAL_PI | `test_general_execution.py::test_locked_pi_warm_rebind_no_extra_model_call` 短答没有额外接单或进度模型请求。 |
| A35 | UNVERIFIED | 尚缺长任务在完成前主动发送两条有事实依据的自然语言反馈的真实 pi 用例。 |
| A36 | UNVERIFIED | 尚缺无新信息的沉默提醒及不刷重复心跳的验收。 |
| A37 | PARTIAL | `test_general_execution.py::test_final_supersedes_only_normal_progress` 确认最终结果替代普通进度并保留重要回执；问题通知由等待链路单独保留。真实网络队列竞争未验收。 |
| A38 | PARTIAL | `tests/contracts/test_adapters.py::test_D02_D03_unknown_and_expired` 覆盖旧 Outbox UNKNOWN/过期边界，通用任务各通知类型的官方 QQ 真机窗口尚未验收。 |
| A39 | REAL_PI | `test_pi_bridge.py::test_locked_pi_blocks_every_member_of_mixed_wait_write_batch` 覆盖等待先后顺序、直接和 capability.invoke 包装，整批每项被拦截。 |
| A40 | PARTIAL | `test_tasks_control.py::test_wait_answer_race_continues_same_task_once` 与 `test_general_execution.py::test_question_fast_reply_is_consumed_after_quiescence_once` 检查持久续接。真实 pi 提问、送达、回答、下一 run 尚缺整链验收。 |
| A41 | FIXTURE | `test_tasks_control.py::test_question_reply_mapping_disambiguates_multiple_waits` 检查多问题 reply-to 映射。 |
| A42 | FIXTURE | `test_tasks_control.py::test_wait_answer_race_continues_same_task_once` 检查重复答案仅一次消费；外部 webhook 仍需单独验收。 |
| A43 | PARTIAL | 取消优先测试通过；`test_general_execution.py::test_locked_pi_late_input_survives_settled_boundary` 的 active/settled 两种真实 pi 用例确认已处理输入才 ACK，未处理输入进入同 Task 下一 run。真实外部活跃写入取消仍需验收。 |
| A44 | FIXTURE | `test_general_execution.py::test_external_wait_has_no_model_poll_and_wakes_once` 验证同 Task 唤醒且观察不调用模型；`test_malformed_external_observation_does_not_kill_other_waiters` 确认空数组等坏观察不终止其他等待。真实 CI 服务未连接。 |
| A45 | FIXTURE | `test_context_execution.py::test_topic_references_weather_followup_and_return_to_displayed_pr` 验证 PR→天气→明天的地点/话题继承和新鲜观察契约。 |
| A46 | FIXTURE | 同一话题引用测试将拆为多条消息的 PR 列表作为一组展示，返回之前第二项时仍绑定原始 PR #52；歧义编号测试拒绝猜目标。 |
| A47 | FIXTURE | `test_provider_gateway.py::test_tool_schema_included_in_context_gate`、`test_transaction_rejection_precedes_reservation` 与 capability 输出截断测试检查预算及完整事务。 |
| A48 | REAL_PI | `test_general_execution.py::test_locked_pi_warm_rebind_no_extra_model_call` 检查第二轮 seed 只存在一次。 |
| A49 | PARTIAL | `test_pi_bridge.py::test_locked_pi_busy_compact_rejected_and_idle_unauthorized_compact_explicit` 确认真正 busy run 不被手工 compact 打断，idle 无授权明确拒绝且不调用上游；未提供可计费的独立 idle 手工压缩管理入口。 |
| A50 | PARTIAL | `test_context_execution.py::test_seed_source_closure_survives_history_trimming_and_revokes`、`test_provider_gateway.py::test_source_revalidation_after_waiting_for_model_slot` 检查来源闭包与出站前重验。真实自动压缩撤权尚未验收。 |
| A51 | REAL_PI | warm 测试比较两轮实际首条消息；`test_provider_gateway.py::test_actual_request_metrics_keep_stable_prefix_and_host_model` 验证出站请求字节数、估算输入、工具大小及稳定前缀/整请求指纹，保留 usage 原值。跨真实配置版本的性能比较未开展。 |
| A52 | PARTIAL | `test_provider_gateway.py::test_usage_raw_and_cache_accounting`、真实 SSE 首块与未知 usage 测试保留原始计数。真实计费价格、费用和 usage coverage 统计尚未验收，缺失费用保持 unknown。 |
| A53 | PARTIAL | `test_provider_gateway.py::test_business_budget_uses_tool_free_bounded_completion` 检查收尾额度，后台化不增预算及多次续配审计尚缺完整用例。 |
| A54 | FIXTURE | `test_general_execution.py::test_no_file_result_detach_and_protocol`、`test_tasks_control.py::test_expired_lease_never_reexecutes`、`test_worker_lost_result_response_replays_without_model` 覆盖旧协议拒绝、fencing 和 spool。 |
| A55 | PARTIAL | `test_general_execution.py::test_no_file_result_detach_and_protocol` 检查无文件自然正文入 Outbox，复杂业务结论与账本一致性仍需真实用例。 |
| A56 | PARTIAL | Context 测试覆盖独立交互/后台开关；`test_general_execution.py::test_pool_host_limits_disable_reuse_and_failed_warm_binding_starts_cold` 确认 warm 关闭后不留 idle、故障停止后冷启动。生产配置回退尚未演练。 |
| A57 | FIXTURE | `test_general_execution.py::test_question_fast_reply_is_consumed_after_quiescence_once`、`test_tasks_control.py::test_wait_answer_race_continues_same_task_once` 检查先持久化、quiescent 后单次唤醒。 |
| A58 | PARTIAL | `test_capabilities.py::test_cancelled_cli_waits_for_parent_and_kills_descendants` 检查 gh/git 取消时父进程与后代停止，含 leader 已退出情形。spawn 中取消、重复取消和 process.wait 取消有定点检查。当前不支持 Task 级 ProcessLease 移交，cleanup 未完成时 run 仍活跃。目标 Linux namespace/cgroup 停止证明尚未验收。 |
| A59 | REAL_PI | 真实 pi 自动压缩测试验证 native 软阈值与 recent 映射、独立 compaction kind/输出上限、压缩后 business 请求通过真实字节边界；移除重复 token 估算门禁；usage 是本地模拟值。 |
| A60 | PARTIAL | `test_general_execution.py::test_continuation_keeps_task_budget_and_completion_reserve` 验证跨 run 不重置、耗尽后只剩有限收尾额度。真实多次续配、通知/压缩计费整链尚未验收。 |
| A61 | PARTIAL | `test_general_execution.py::test_question_timeout_starts_after_delivery` 与 `test_unknown_question_delivery_never_starts_user_timeout` 覆盖 SENT/UNKNOWN 计时及投递过期原因；引用回答另有映射检查；管理 API 人工确认 UNKNOWN→SENT 后也在同一事务启动计时，并能查询任务通知。官方 QQ 真实送达窗口未验收。 |
| A62 | PARTIAL | `test_capabilities.py::test_git_push_uses_sanitized_repo_and_verifies_remote` 检查不继承 workspace Git helper，`test_git_clone_followup_fetch_push_use_same_bounded_path` 检查统一路径与 NOFOLLOW。process 测试检查 bubblewrap argv 与 run 归属。真实 Linux 凭据、helper、agent、/proc 隔离以及真实 clone/edit/commit/push 尚未验收。 |

## 部署验收边界

真实 GitHub 写操作应限定在用户已授权的测试仓库，保存远端对象 ID、账号、提交 SHA、回读时间与不确定状态。目标 Linux 需实际验证 bubblewrap、systemd、namespace、后代清理、服务凭据不可见及受控 Git 工作流。QQ 需要实际平台送达和过期窗口证据。上述检查完成后再更新对应状态与结果，源码或 fixture 结果不能补足这些证据。

## 已知实现限制

- 宿主命令进程暂归当前 run 所有；不支持将活跃进程移交给下一 run。结束前须等待或终止。
- Worker 硬崩溃时，pi 的停止 marker 不足以证明 gh/git 已停止。缺少可信的宿主工具 drain 证据时保留 UNKNOWN 和执行槽，禁止盲目恢复或重放。
- idle 手工压缩没有独立授权/计费管理入口；普通执行中的自动压缩由 pi 原生实现。
- 缺少实际模型价格与远端账单时费用保持 unknown；默认预算、真实业务自然语言质量及峰值延迟仍需在目标部署评估。

## home 部署验证（2026-11-09）

已部署至 `/opt/qqbot`，沿用 `elephant-core-acceptance.service` 和 `elephant-worker-acceptance.service`。数据库从 0005 升至 0007，完整性检查通过；部署前备份位于 home 的 `/root/elephant-deploy-backups/20261109T055101Z`，包含旧代码、配置、数据库、Worker 状态和启动配置。NapCat 未重启。

目标主机十八项隔离探针通过。另以实际 Worker 身份和服务沙箱执行工作区命令，验证供应商环境变量、服务配置、Core 数据及用户主目录不可见。真实 pi 1.5.0 + OpenCode Go 完成连续两轮简短回复，同一进程复用、每轮一次模型请求、usage 有上游报告，最后停止清理成功。该检查使用独立测试数据库，没有向 QQ 发送消息。

实际供应商验收发现通用代理缺少 `x-opencode-session`，已补回旧研究代理已有的会话头，并在 warm rebind 时保持供应商会话身份；本地两项定点回归及真实两轮测试通过。Core live/ready 均返回 200，OneBot 连接正常，Core/Worker 无重启循环。

后续已按用户要求取消每分钟回复数量限制，并优先使用 home 本地 gh 登录凭据，经 systemd LoadCredential 提供给 Worker。连续发言改为同一会话内 1.2–2.8 秒抖动间隔；间隔持久化，其他会话不受阻塞。真实 GitHub 只读查询已通过；本次没有执行 GitHub 写入、Worker 硬崩溃恢复或主机重启。


## 1M 上下文与 QQ 输出修复（2026-11-09）

home 的模型上下文窗口现为 1,000,000，压缩阈值和保留长度按窗口推导，移除了独立的保守估算 token 拦截；请求字节与实际供应商错误边界仍保留。工具提供自然关键词发现、完整能力目录与有效内容引用，权限维持 allow_all。普通交互默认预算为 30 次模型、80 次工具、600 秒，后台为 60 次模型、200 次工具、1,800 秒。停止和结果落盘期间继续续租；永久拒绝的 spool 被隔离，不再阻塞之后的领取；失败说明使用独立有限预算。供应商收尾请求保留工具定义并禁用调用，非法工具协议文本拒绝进入 QQ。

QQ 的普通回复、命令、进度、提问和结果统一使用纯文本输出链路。模型提示与默认人格使用自然的第一人称大象口吻；确定性转换保留完整网址和代码正文。超长内容依次优先按段落、换行、句子、空白分段，拼接后不丢字符；只有超过单条上限且无可用边界的内容才兜底切分。纯文本编号也参与后续展示引用识别。

本地验证覆盖上下文、工具、provider、Worker 失败、任务控制及 QQ 输出链路，失败项已修复并定点复验。新增 31 项 QQ 输出测试通过，包含语义分段、字符保留、持久间隔、并发发送和重试；假时钟 WebSocket 验收同步适配发送间隔。未重复运行云端 CI。

部署后的独立数据库探针使用服务器安装代码和真实时钟，将三段内容完整分开发送到测试接收器，间隔分别为 1.69 秒和 1.47 秒；重建发送器后间隔仍有效。探针没有连接 QQ，真实多条 QQ 网络交付间隔仍须与平台耗时区分。

真实任务 T-d066b72f-a6e 查询明确指定的 YourTongji/YourTJ-Hub，得到 16 个开放 PR。两次业务模型请求、一次 GitHub 工具调用后成功；1,739 字纯文本完整交付，QQ 回执为 SENT（平台消息 ID 424261131），执行槽已释放。先前仅用模糊名称 yourtj 的查询曾选错组织；明确仓库 URL 的流程已验证，不能据此宣称模糊目标解析已完全解决。

部署备份位于 home 的 `/root/elephant-deploy-backups/20261109T073219Z-context-fixes`、`20261109T073756Z-completion-fix` 和 `20261109T080641Z-qq-presentation`。Core/Worker 正常运行且无重启循环。完整 1M 输入压力、QQ 用户阅读确认、真实 GitHub 写操作尚未验收。
