# ADR 0003 durable tasks and isolated capabilities

Accepted for task control and Pi execution. Each target requires its own Linux isolation and real research/report delivery acceptance.

Core owns task/run/event state and Outbox. An independent supervisor uses an authenticated internal API and bounded fsynced result spool. Persistent runner slots survive uncertain termination. Leases and fencing reject stale writes, while unconfirmed runner death forces reconciliation before any replacement run.

Pi receives an exact session path, one run per process and an explicit no-builtin-tools research extension. Host-side public fetching, artifact export and fixed-model forwarding enforce the trust boundary. The runner receives temporary run-scoped tokens. Provider keys and the internal Core token stay in the trusted supervisor.

Queued task cancellation is immediate. Running cancellation blocks subsequent proxy operations and proceeds through clear_queue, abort, process-group TERM/KILL and stop verification. Result completion requires a final settled reply, a host-exported artifact and host-retrieved source evidence. This verifies deliverables and provenance, while research correctness still needs human review.

A direct same-UID child is retained solely for local protocol development. Production mode invokes a fixed privileged launcher and validated manifest, with separate UID, root-directory binds, cgroup lifecycle and a restricted network namespace. Its policy refuses startup until a target Linux install proves these boundaries. The installation policy starts unverified. The fixed root-only probe enables it only after its boundary checks pass. Automatic session replay and ambiguous input redelivery remain disabled.
