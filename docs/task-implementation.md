# Task implementation

Core `TaskService` is the sole database writer. An open-ended `TaskGoal` records the user's target, constraints, requested actions, success checks and optional outputs. `ExecutionPlan` records pi, resolved budgets, workspace/config references and initial delivery. ATTACHED/DETACHED delivery is independent of task execution, tool capabilities and whether files are required. Task acceptance persists the goal and plan, without adding a fixed confirmation message.

Ordinary conversation and explicit background requests use the same pi Worker path. The deterministic planner gives initial delivery and retains legacy verification checks. Pi discovers business workflows from its native skill directory and loads their bodies when needed. Foreground timeout changes delivery on the existing Task/run and preserves its existing process, session and tool history. A queued Task detaches without creating a fictitious run. `interactive_execution_enabled` controls ordinary pi submissions. `tasks_enabled` enables the explicit `/研究` command. Actual execution requires a running Worker in either case.

## Authority, scheduling and lifecycle

`TaskService` accepts injected permissions, active/sustained capacity, context validation and budget ceilings. The default application policy is `allow_all`. Conversation and identity come from trusted adapter context. Legacy role and allowlist metadata remain available, while actual operations use `PermissionService`. Current-conversation lookup and legal source selection still apply. Authentication, QQ platform restrictions, current run/fence, credentials and budgets are checked separately.

Default Core capacity has four ordinary active-run slots and one finite interactive overflow slot. The overflow admits an interactive run only after all occupied ordinary runs have background priority. New background runs remain limited to two. Execution priority is persisted separately from delivery and refreshed at model-request boundaries. Model capacity has the same bounded reserve, including legacy and general runners in one Worker. Queue limits are eight per requester and twenty per conversation. Conversation fairness and submission wakeups share the existing SQLite task queue. Worker bounds concurrent supervision, shares model capacity and separately manages its resident/idle process limits. Align Core settings with Worker CLI capacity on deployment.

Each active session has one run owner. Binding generation and credentials rotate before each run's input/compaction. A committed quiescent run can return its process to IDLE. Real termination releases resident capacity and records a stop proof. Unconfirmed stops retain ownership as UNKNOWN or NEEDS_RECONCILIATION. Expired fencing never commits a new terminal result. Reconciliation requires proved stopping and new recovery ownership before adopting a spool result. Retry creates a linked Task and shares the original durable operation lineage, so uncertain remote writes are not submitted again under a new task ID. A pi marker alone cannot release a generic run while host gateway/tool stopping is unverified.

`TaskService` exposes task acceptance/query/control, claim/renew/event/result submission, notifications, durable questions/answers, continuation consumption, budget reservation/usage, process binding/stop, observations, operation ledger and external-wait observations. The authenticated internal router lives beneath `/internal/tasks/`. Worker payloads include worker identity, run ID and fencing token. The internal token is at least thirty-two characters and is available only to Core and Worker. Production exposes the router through its protected Unix socket.

## Goals, effects and delivery

A completed run can satisfy a goal with an empty artifact/evidence list. Explicitly requested outputs still require actual verified archive files. Named success checks require registered verifiers and recorded observations or effects. Partial, blocked and unknown external effects remain distinguishable from completed goals. An approval needs a submitted review and verified identity/commit, a rerun acceptance differs from passed CI, and auto merge differs from a completed merge.

The Core-owned ActionLedger records PREPARED before dispatch, then DISPATCHED and VERIFIED/PARTIAL/FAILED/UNKNOWN. Stable operation IDs suppress retransmission. Matching unsafe GitHub effects and Git push also share semantic protection across call IDs and task retries. New local process or fetch calls remain available after workspace changes. Uncertain remote effects require readback before further writes.

Agent-authored progress, questions, status explanations and final results enter the existing Outbox with distinct notification identities and stable content. Accepted enqueue and successful execution do not prove platform delivery. Send retries preserve the same notification and body. Ordinary progress can be superseded by later progress or a final result. Questions and important receipts keep their own delivery state.

`task.ask_user` persists question/checkpoint and wait intent. The extension requires a waiting tool to occupy its whole batch. Later business effects are blocked while pi settles. Answers are source-deduplicated and mapped to the persisted expected responders and question delivery. In groups, the requester must quote the question, mention the bot or explicitly name the Task with `/补充`; another permitted member may answer by quoting the delivered question. Private conversations accept an ordinary answer only when there is one unambiguous wait. Expired, recalled and older-than-question events are rejected by the TaskService as well as the automatic router. An explicit supplement to an external wait is retained until the actual external observation satisfies that wait. A fast answer stays pending until quiescence, then one continuation revision consumes it and starts the same Task's next run. Cancellation suppresses late continuation. Delivery and answer deadlines are separate.

`task.wait_external` persists a registered read-only remote observation condition. Deterministic bounded polling rechecks authority and observes the external resource without invoking a model each poll. A satisfied condition is durably recorded before the Task resumes. Failures or stale observations do not satisfy the condition. Local processes cannot be detached from their run without a Task-level ProcessLease handoff, which is currently unsupported.

## Worker configuration and recovery

Run `python -m qqbot.jobs.worker --socket SOCKET --token-file FILE --pi ABSOLUTE_PI --workspace ROOT --spool ROOT --archive ARCHIVE --runner-mode systemd --model MODEL --model-endpoint CHAT_COMPLETIONS_URL`. Worker model selection and endpoint are explicit CLI values. The provider key comes from its dedicated `QQBOT_RUNNER_OPENAI_KEY`. Core's model configuration and Worker values should describe the same deployed model. Pi receives rotating temporary tokens rather than the provider key or an operator profile.

`--concurrency`, `--max-idle`, `--idle-ttl` and `--model-context-window` control Worker capacity/model limits. `--repository`, `--github-host`, `--gh` and `--github-token-env` configure host-bound GitHub access. Credentials first use `gh auth token --hostname` from the Worker account (respecting `GH_CONFIG_DIR`), then fall back to `--github-token-env`. The local lookup ignores GitHub token environment overrides. `--workspace-binding NAME=PATH` supplies an explicit host workspace binding. Otherwise a Task-specific workspace is created under the Worker's state directory. Only the bound workspace is exposed to business tools. The archive root must match Core `TaskService.archive_root`.

Results and bounded execution events are fsynced to a twenty MiB Worker spool before delivery to Core. Event IDs and contiguous sequences support identical replay. Lost result responses can be replayed without rerunning pi or external actions. Conflicting replay fails. Stale spool remains recovery evidence until authenticated reconciliation or confirmed discard. The spool does not become another domain database.

Production uses the fixed root launcher with independent process/session identity and `pi_bridge.ts`. The launcher and root stop markers preserve UID, namespace, cgroup and filesystem boundaries. `--runner-mode local` supports protocol development and has no verified production cgroup boundary. Credentialed GitHub and workspace subprocess capabilities additionally require bubblewrap. Refer to [deployment instructions](../deploy/README.md) and [harness contracts](harness-contract.md).

Actual verification coverage and unfinished external acceptance appear in [the A01–A62 matrix](agent-execution-acceptance.md). Real QQ windows/delivery, GitHub writes, model billing, Linux isolation and reboot recovery must be checked on their corresponding systems.

Core includes pool limits in each claim context. `warm_max=0` disables idle reuse while retaining cold execution capacity; otherwise resident capacity is at least `max_active + 1`. Idle count and TTL follow the current context configuration. Workspace aliases are serialized while any owning run retains its execution slot.


Context windows and execution limits
-----------------------------------

Set Core `model.context_window` and Worker `--model-context-window` to the actual model
window (for the home deployment: `1000000`). `context.max_tokens` and
`context.keep_recent_tokens` default to `null`, selecting pi thresholds from that
window; `max_request_bytes` is an independent 16 MiB transport limit. The former
`hard_input_tokens` and `compaction_input_tokens` estimate gates have been removed.
Warm runners must be restarted when model or context limits change. Skill updates retire idle processes by pinned content version.

Interactive defaults allow 30 model requests, 80 tool calls, 600 active seconds,
and 4,096 output tokens per call. Background defaults allow 60 model requests,
200 tool calls and 8,192 output tokens per call. These are ceilings, not targets;
ordinary conversation still needs only one model call. Application permission
remains `allow_all` unless explicit rules override it. Pi receives the native skill
directory and uses controlled resource reads and the shared execution bridge.

Warm context increments
-----------------------

The trusted bridge injects the initial seed once, then sends `context_delta` with
new or revised history sources and the current topic and memory metadata. Human
events and delivered bot messages use their stable source IDs and content hashes.
The source cursor is saved in the same pi custom-message entry as the context, so
rebinds and process recovery acknowledge only context actually persisted in the
session. Trimming a source out of the current selection does not erase its cursor.
The existing source closure, permission checks and epoch invalidation still govern
warm reuse and requests after retraction or configuration changes.


Task routing and skill versions
------------------------------

A thin deterministic router maps explicit references, task numbers and the same
requester's clear supplements to existing Task inputs and pi steer. Multiple
candidates require a task selection. Explicit independent questions use normal
submission. A terminal continuation retains the original source closure and Task
reference. Continuing unfinished external work retains its operation lineage and pinned skill
version, including previously verified steps. A completed task with a fresh explicit
operation can use a new lineage.

Worker validates an immutable content-addressed skill snapshot before creating a
runner. `bind-skills` fixes that version in `context_json`. Repeated binding is
idempotent and a conflicting version is rejected. Pool identity includes the skill
version. A directory update retires old idle processes, while busy tasks finish
with their original resources. Waiting tasks can cold-start their older version.
A missing pinned version fails with one host-authored Outbox notice.

The maintained skill source is separate from snapshots. Production versions live
under root-owned `/opt/qqbot/skillsets/<sha>` and are mounted read-only at `/skills`
in both pi and workspace process environments. Local discovery paths are mapped to
the same virtual paths. The `read` bridge resolves relative workspace files against
the task execution workspace. Native system-prompt sections retain skill discovery
alongside the host persona. Skill reads record the version, path and content hash.
Legacy account adapters, external wait recipes and semantic write deduplication
remain available behind the common bridge for stored-task compatibility.
