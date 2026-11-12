"""Host-owned memory scope and raw-source contracts, independent of a backend."""
from typing import Literal, Protocol

from pydantic import Field, model_validator

from qqbot.domain import DTO, stable_id


class MemoryScope(DTO):
    owner_id: str = Field(min_length=1, max_length=128)
    workspace_id: str = Field(default="personal", min_length=1, max_length=128)
    audience: Literal["private", "project", "group"] = "private"

    @property
    def key(self):
        return stable_id(self.owner_id, self.workspace_id, self.audience)


class MemoryBinding(DTO):
    """Resolved by host configuration, never a model-supplied credential selector."""
    owner_id: str
    workspace_id: str = "personal"
    account_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    user_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    api_key_env: str = Field(min_length=1, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")

    @property
    def root_uri(self):
        return f"viking://user/{self.user_id}/memories"


class SourceEvent(DTO):
    """An original event exported by Harness, not a compaction or notification seed."""
    id: str = Field(min_length=1, max_length=256)
    seq: int = Field(ge=1)
    role: Literal["user", "assistant", "tool"]
    content: str = Field(max_length=64000)
    revision: str = Field(default="1", min_length=1, max_length=128)
    source_event_ids: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    kind: Literal["original", "verified_fact", "relay", "summary", "tool_output", "skill_review"] = "original"
    created_at: str | None = None

    @property
    def origins(self):
        return self.source_event_ids or (self.id,)


class SourceBatch(DTO):
    scope: MemoryScope
    session_id: str = Field(min_length=1, max_length=256)
    from_seq: int = Field(ge=0)
    to_seq: int = Field(ge=1)
    events: tuple[SourceEvent, ...] = Field(max_length=100)
    branch: str = Field(default="main", min_length=1, max_length=128)
    projection_version: str = "pi-original-v1"
    policy_revision: str = "memory-v1"

    @model_validator(mode="after")
    def ordered_original_events(self):
        if self.to_seq <= self.from_seq:
            raise ValueError("memory source range is (from_seq, to_seq]")
        positions = [event.seq for event in self.events]
        if positions != sorted(set(positions)) or any(not self.from_seq < seq <= self.to_seq for seq in positions):
            raise ValueError("source events must be unique, ordered and inside their export range")
        if len({event.id for event in self.events}) != len(self.events):
            raise ValueError("duplicate source event id")
        return self


class SyncReceipt(DTO):
    operation_id: str
    state: str
    accepted: bool
    indexed: bool = False
    extracted: bool = False
    exported_seq: int = 0
    indexed_seq: int = 0
    extracted_seq: int = 0
    memory_epoch: int = 0
    remote_task_id: str | None = None
    changed_uris: tuple[str, ...] = ()
    error_type: str | None = None


class MemoryHit(DTO):
    uri: str
    score: float = 0
    source_event_ids: tuple[str, ...] = ()
    revision: str
    updated_at: str


class MemorySearchResult(DTO):
    status: Literal["ready", "disabled", "degraded", "blocked"]
    hits: tuple[MemoryHit, ...] = ()
    memory_epoch: int = 0
    error_type: str | None = None


class MemoryDocument(DTO):
    uri: str
    content: str
    source_event_ids: tuple[str, ...]
    revision: str
    updated_at: str
    memory_epoch: int
    trust: Literal["retrieved_data"] = "retrieved_data"


class MemoryCorrection(DTO):
    scope: MemoryScope
    uri: str
    session_id: str
    source: SourceEvent
    idempotency_key: str = Field(min_length=1, max_length=256)
    projection_version: str = "pi-original-v1"
    branch: str = "main"


class MemoryDeletion(DTO):
    scope: MemoryScope
    idempotency_key: str = Field(min_length=1, max_length=256)
    uris: tuple[str, ...] = ()
    source_event_ids: tuple[str, ...] = ()
    delete_archives: bool = False

    @model_validator(mode="after")
    def bounded_target(self):
        if not self.uris and not self.source_event_ids:
            raise ValueError("forget needs explicit memory URIs or original source IDs")
        if len(self.uris) + len(self.source_event_ids) > 128:
            raise ValueError("memory deletion target exceeds bound")
        return self


class MutationReceipt(DTO):
    operation_id: str
    state: str
    completed: bool
    memory_epoch: int
    changed_uris: tuple[str, ...] = ()
    affected_source_ids: tuple[str, ...] = ()
    error_type: str | None = None


class MemoryPort(Protocol):
    async def sync(self, batch: SourceBatch) -> SyncReceipt: ...
    async def poll(self, operation_id: str) -> SyncReceipt | MutationReceipt: ...
    async def search(self, scope: MemoryScope, query: str, *, limit: int = 8) -> MemorySearchResult: ...
    async def read(self, scope: MemoryScope, uri: str) -> MemoryDocument: ...
    async def correct(self, request: MemoryCorrection) -> MutationReceipt: ...
    async def forget(self, request: MemoryDeletion) -> MutationReceipt: ...
