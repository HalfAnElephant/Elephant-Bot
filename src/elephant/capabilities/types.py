"""Host-owned capability contracts. Model input never supplies execution identity."""
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol


@dataclass(frozen=True)
class ToolContext:
    task_id: str
    run_id: str
    fence: int
    principal: Any
    workspace: str
    repository: str | None = None
    host: str = "github.com"
    requested_actions: tuple[str, ...] = ()
    environment: Literal["sandbox", "host"] = "sandbox"


@dataclass(frozen=True)
class Action:
    name: str
    resources: tuple[str, ...]
    effect: str = "read"


@dataclass
class ToolResult:
    status: str
    data: Any = None
    error: str | None = None
    operation_id: str | None = None
    retrieved_at: str | None = None
    cursor: str | None = None
    truncated: bool = False
    content_ref: str | None = None
    effect_refs: tuple[str, ...] = ()
    observation_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class Capability:
    name: str
    description: str
    parameters: dict = field(default_factory=dict)
    version: str = "1"
    timeout_seconds: int = 30
    max_output_bytes: int = 16384
    cancellable: bool = True


class ActionLedger(Protocol):
    async def prepare(self, operation_id: str, context: ToolContext, action: Action,
                      arguments_hash: str, arguments: dict) -> dict:
        """Atomically insert PREPARED or return existing record. Must compare hash and scope."""
        ...

    async def transition(self, operation_id: str, expected: str, state: str, result: dict) -> bool:
        """Durable compare-and-swap. DISPATCHED is committed before any external call."""
        ...
