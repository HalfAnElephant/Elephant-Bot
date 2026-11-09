# Pi harness contract

The locked pi 1.5.0 RPC executable is the model/tool loop for ordinary conversation and durable tasks. `PiRunnerAdapter` invokes an absolute argv and an exact JSONL session path. A resident process owns a session, while each active run receives a fresh host binding. The binding includes process generation, task/run identity, fencing, current authority and separate model/tool tokens. Ambient HOME, operator credentials, built-in tools, extensions, context files, skills, MCP and prompt templates are excluded. New runs load the trusted `pi_bridge.ts` extension.

The stdout reader starts before prompt submission. Records split on LF bytes, preserving Unicode U+2028/U+2029. Events are limited to 2 MiB and the queue to eight records. stderr is drained with a 20 MiB ceiling. Truncated, malformed or oversized records fail the run. Prompt ACK and `agent_end` cannot complete it. A handled prompt is rejected explicitly. Completion requires `agent_settled`, an empty pi queue, drained I/O and revoked run authority. User-visible completion also depends on the goal's requested observations, effects and outputs. Artifacts and source evidence may be empty.

## Run and process ownership

A quiescent run can release its execution slot while its process remains IDLE. Resident capacity is released after verified process termination. The pool is bounded and evicts idle entries by TTL or capacity. Unknown processes retain ownership for reconciliation. The direct local runner has no verified cgroup boundary. Production uses a dedicated runner UID, fixed systemd launcher and root-owned unit/cgroup stop evidence.

Cancellation revokes the binding, stops new external calls, sends `clear_queue` followed by `abort`, then terminates the process when required. Model and tool HTTP operations pin their initiating binding until drained. Controlled subprocess launch and teardown await cancellation cleanup, kill the process group and reap the child, including cancellation during launch and a leader exiting before its descendants. Bubblewrap namespace and descendant behavior still require target-host acceptance.

Active-run input can be persisted and steered at a safe pi boundary. A question or external wait saves a durable continuation and lets the run settle. Input arriving during that transition stays pending until quiescence, then starts one next run of the same Task. Exact arbitrary-step crash resume is unsupported. Session references support exact inspection and explicit recovery. The adapter never uses fuzzy `--continue` or blindly resends an uncertain prompt.

## Capability gateway

Pi sees a small stable bridge for capability discovery/invocation, content paging, notifications, waiting and outcomes. Direct and discovered tools use the same `ToolGateway`. Principal, workspace, repository, host, goal, run and fence come from the host. Every invocation rechecks current Core authority and permissions. Default application policy is `allow_all` with no rules. Configured restrictions still apply to actual actions and resources. Unknown GitHub operations cannot bypass relevant restrictions.

`web.search` and `web.fetch` reuse the public HTTPS fetcher. DNS answers must all be globally routable, the connection uses a checked IP with TLS hostname validation, and redirects are checked again. Fetch uses HTTPS port 443, bounded text-like responses, identity encoding and a 1 MiB page cap. Workspace file access uses descriptor-relative `O_NOFOLLOW` traversal. Large results are bounded, carry explicit truncation and can be read through run-bound content references.

`gh.exec` uses literal argv and stdin. Credentialed execution requires Linux bubblewrap and a host-bound token, with a private process/mount view and a trusted empty working directory. `git.transport` provides controlled clone/fetch/push against a bounded relative `path`. It omits workspace config, hooks and credential helpers. Clone returns a relative path usable by later fetch and push. Push requires a full commit SHA and destination branch, then reads the remote ref back. Ordinary `process.start/read/wait/terminate` commands have their own workspace namespace, empty environment and disabled network. Live processes remain run-owned and prevent quiescence. Task-level ProcessLease transfer is unsupported.

Side effects are persisted before dispatch. Operation-ID replay never repeats an effect. Semantic deduplication across different call IDs applies to unsafe GitHub operations and Git push. Repeated local commands and Git fetch with new IDs remain executable. UNKNOWN, PARTIAL, DISPATCHED and PREPARED remote operations require reconciliation. A zero CLI exit status does not establish a verified remote effect. Review verification checks actor, ID, state, submitted time and commit. Commit-bound reviews use the REST `commit_id` field. Auto merge and completed merge have distinct evidence.

Waiting tools must be alone in their entire pi tool batch. The extension resolves capability wrappers before preflight, and rejects every member of a mixed wait/write batch. Notification enqueue returns accepted with an independent notification ID. Outbox records actual delivery separately.

## Provider transport and budgets

The trusted Worker fixes model and endpoint. Provider credentials remain outside pi. Application byte/token gates include messages, tool schemas and complete tool transactions. Ordinary input defaults to an 8K hard estimate, with bounded independent compaction input/output and at most two compactions per run. Model and tool calls consume the Task's persisted budget. A reserved tool-free completion request can naturally describe partial or unknown results when business budget is exhausted. Pi remains the only retry and execution-history compaction owner.

Supported upstream SSE is passed through as it arrives. Parsing for accounting preserves original bytes, unknown usage fields and raw provider counters. Keepalive frames do not count as first tokens. Missing token or cost fields remain unknown. Source authorization is checked again after waiting for model capacity and before outbound requests, as well as while streaming. Unsupported stream protocols fail explicitly. The legacy `ResearchCapabilities` proxy remains for compatibility with its original protocol and quotas.

## Artifact export

Export supports default MIME mappings for Markdown, text, JSON, CSV, HTML, ZIP, PDF, PNG, JPEG and WebP. A caller can supply a narrower or extended MIME map. Binary files retain exact bytes. Text-like types and JSON require UTF-8. Descriptor-relative opens reject symlinks, non-regular files and hardlinks. Count, total bytes, stable size/mtime and archive SHA-256 are checked. Failed exports remove their created archive entries. Core accepts only verified UUID archive references and checks actual size/hash.

The local deterministic and locked-pi checks, remaining external acceptance and known gaps are tracked in [the acceptance matrix](agent-execution-acceptance.md). Production isolation, actual provider billing, real GitHub writes and QQ delivery require their own evidence.
