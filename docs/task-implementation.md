# Task implementation

Core `TaskService` is the sole database writer. An open-ended `TaskGoal` records the user's target, constraints, requested actions, success checks and optional outputs. `ExecutionPlan` records pi, resolved budgets, workspace/config references and initial delivery. ATTACHED/DETACHED delivery is independent of task execution, tool capabilities and whether files are required. Task acceptance persists the goal and plan, without adding a fixed confirmation message.

Ordinary conversation and explicit background requests use the same daemon-managed Pi execution pool. The deterministic planner gives initial delivery and capability hints. Pi chooses tools through discovery when useful. Foreground timeout changes delivery on the existing Task/run and preserves its existing process, session and tool history. A queued Task detaches without creating a fictitious run. `interactive_execution_enabled` controls ordinary Pi submissions. `tasks_enabled` enables the explicit `/研究` command. The daemon Scheduler owns run admission and execution; there is no separate Worker service.

## Authority, scheduling and lifecycle

`TaskService` accepts injected permissions, active/sustained capacity, context validation and budget ceilings. The default application policy is `allow_all`. Conversation and identity come from trusted adapter context. Legacy role and allowlist metadata remain available, while actual operations use `PermissionService`. Current-conversation lookup and legal source selection still apply. Authentication, QQ platform restrictions, current run/fence, credentials and budgets are checked separately.

Agent and task admission have no fixed count cap. The daemon Scheduler applies model request capacity separately and reserves one request slot for Leader work when total capacity exceeds one. Pi process reuse is managed inside that same Scheduler; no Worker CLI capacity needs to be synchronized with Core.

Each active session has one run owner. Binding generation and credentials rotate before each run's input/compaction. A committed quiescent run can return its process to IDLE. Real termination releases resident capacity and records a stop proof. Unconfirmed stops retain ownership as UNKNOWN or NEEDS_RECONCILIATION. Expired fencing never commits a new terminal result. Reconciliation requires proved stopping and new recovery ownership before adopting a spool result. Retry creates a linked Task and shares the original durable operation lineage, so uncertain remote writes are not submitted again under a new task ID. A pi marker alone cannot release a generic run while host gateway/tool stopping is unverified.

`TaskService` exposes task acceptance/query/control, claim/renew/event/result submission, notifications, durable questions/answers, continuation consumption, budget reservation/usage, process binding/stop, observations, operation ledger and external-wait observations. The daemon Scheduler uses an in-process `RuntimeClient` to call the task and Leader services. There is no Worker HTTP route or Unix socket listener. The `/internal/tasks/…` labels the Worker passes to `RuntimeClient` are in-process operation names, not network endpoints. External task management uses the protected admin API and CLI.

## Goals, effects and delivery

A completed run can satisfy a goal with an empty artifact/evidence list. Explicitly requested outputs still require actual verified archive files. Named success checks require registered verifiers and recorded observations or effects. Partial, blocked and unknown external effects remain distinguishable from completed goals. An approval needs a submitted review and verified identity/commit, a rerun acceptance differs from passed CI, and auto merge differs from a completed merge.

The Core-owned ActionLedger records PREPARED before dispatch, then DISPATCHED and VERIFIED/PARTIAL/FAILED/UNKNOWN. Stable operation IDs suppress retransmission. Matching unsafe GitHub effects and Git push also share semantic protection across call IDs and task retries. New local process or fetch calls remain available after workspace changes. Uncertain remote effects require readback before further writes.

Agent-authored progress, questions, status explanations and final results enter the existing Outbox with distinct notification identities and stable content. Accepted enqueue and successful execution do not prove platform delivery. Send retries preserve the same notification and body. Ordinary progress can be superseded by later progress or a final result. Questions and important receipts keep their own delivery state.

`task.ask_user` persists question/checkpoint and wait intent. The extension requires a waiting tool to occupy its whole batch. Later business effects are blocked while pi settles. Answers are source-deduplicated and mapped by reply-to when possible. A fast answer stays pending until quiescence, then one continuation revision consumes it and starts the same Task's next run. Cancellation suppresses late continuation. Delivery and answer deadlines are separate.

QQ task controls bind replies and explicit task references to the original conversation and requester. Use `补充 T-… 内容` or `回答 T-… 内容` with a task reference from the console, or reply to a task message. An answer must identify an open question. If the user chooses from several questions, the answer and the selection message are both required sources. A task-level follow-up stays with the selected Worker, and a goal-level follow-up reaches its active children. A durable control receipt preserves the target when ingress retries. Pi acknowledges each input for the current run after its trusted user message is present in the native session. Revoked input sources fence subsequent execution and delivery that use those sources.

The QQ adapter records authenticated deletion events for guild channels and guild direct messages. Receiving those events depends on the bot's configured subscriptions. The [official group and C2C event list](https://bot.q.qq.com/wiki/develop/api-v2/dev-prepare/interface-framework/event-emit.html) provides message creation and subscription events for those surfaces.

Task delivery progress reads Outbox states for the result. A completed goal remains queryable while QQ delivery waits for an authorized trigger. Its result counts as delivered after every part of a result notification has a send receipt.

`task.wait_external` persists a registered read-only remote observation condition. Deterministic bounded polling rechecks authority and observes the external resource without invoking a model each poll. A satisfied condition is durably recorded before the Task resumes. Failures or stale observations do not satisfy the condition. Local processes cannot be detached from their run without a Task-level ProcessLease handoff, which is currently unsupported.

## Daemon execution and recovery

Production starts one daemon with `elephant serve --config /etc/elephant/core.yaml`. Its Scheduler claims durable task, Leader and Hermes review runs in-process and creates Pi runs from the persisted model binding. There is no standalone `python -m elephant.jobs.worker` command or production Worker listener; `deploy/elephant-worker.service` is retired. See [deployment instructions](../deploy/README.md).

The daemon-owned Worker, spool and Pi runner preserve fsynced result/event replay. Reconciliation must authenticate the run and prove the old process stopped before adopting a result; uncertain external writes still require readback. The spool remains recovery evidence, not another domain database.

Production Pi processes use the fixed root launcher with run-scoped identity, `pi_bridge.ts` and root-owned stop evidence. This preserves UID, namespace, cgroup and filesystem boundaries. Credentialed GitHub and workspace subprocess capabilities additionally require bubblewrap. See [harness contracts](harness-contract.md) for the host boundary and [deployment instructions](../deploy/README.md) for installation.

Actual verification coverage and unfinished external acceptance appear in [the A01–A62 matrix](agent-execution-acceptance.md). Real QQ windows/delivery, GitHub writes, model billing, Linux isolation and reboot recovery must be checked on their corresponding systems.

The first task collaboration milestone requires [routing and replay](../tests/test_host_controls.py) and [Pi input recovery](../tests/test_pi_input_recovery.py). The second requires [parent wakeup, verified-effect recovery and QQ result retriggering](../tests/test_message_updates.py). Direct interaction with a selected work item also requires [scoped attachment handoff](../tests/test_task_input_media.py). These checks use local fixtures and synthetic QQ traffic.

Core includes pool limits in each claim context. `warm_max=0` disables idle reuse while retaining cold execution capacity; otherwise resident capacity is at least `max_active`. Idle count and TTL follow the current context configuration. Workspace aliases are serialized while any owning run retains its execution slot.


Context windows and execution limits
-----------------------------------

Set `model.context_window` to the actual model window. A persisted run binding records
the resolved window for its session. `context.max_tokens` and
`context.keep_recent_tokens` default to `null`, selecting pi thresholds from that
window; `max_request_bytes` is an independent 16 MiB transport limit. The former
`hard_input_tokens` and `compaction_input_tokens` estimate gates have been removed.
Resident Pi sessions must be restarted when these settings or tool catalogs change.

Interactive defaults allow 30 model requests, 80 tool calls, 600 active seconds,
and 4,096 output tokens per call. Background defaults allow 60 model requests,
200 tool calls and 8,192 output tokens per call. These are ceilings, not targets;
ordinary conversation still needs only one model call. Application permission
remains `allow_all` unless explicit rules override it. Pi receives the installed
capability catalog and chooses whether to call, search or page tools itself.
