"""One authoritative OpenViking backend with host-bound USER credentials."""
from urllib.parse import urlsplit

from pydantic import Field, model_validator

from qqbot.contracts.memory import MemoryBinding
from qqbot.domain import DTO


class OpenVikingConfig(DTO):
    endpoint: str | None = None
    api_key_env: str = "QQBOT_OPENVIKING_KEY"
    namespace: str = Field(default="elephant", pattern=r"^[a-z0-9][a-z0-9_-]{0,47}$")
    bindings: tuple[MemoryBinding, ...] = ()
    expected_version: str = "v0.5.4.1"
    private_enabled: bool = True
    group_enabled: bool = False
    timeout_seconds: float = Field(default=2.0, ge=0.1, le=30)
    operation_budget_seconds: float = Field(default=8.0, ge=0.1, le=60)
    max_query_chars: int = Field(default=4000, ge=100, le=8000)
    recall_max_chars: int = Field(default=8000, ge=256, le=32000)

    @model_validator(mode="after")
    def valid_endpoint_and_bindings(self):
        if self.endpoint is not None:
            url = urlsplit(self.endpoint)
            if (url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password
                    or url.query or url.fragment or url.path not in {"", "/"}):
                raise ValueError("OpenViking requires an HTTP(S) origin without credentials or path")
            if url.scheme != "https" and url.hostname not in {"localhost", "127.0.0.1", "::1"}:
                raise ValueError("non-loopback OpenViking requires HTTPS")
        scopes = [(binding.owner_id, binding.workspace_id) for binding in self.bindings]
        identities = [(binding.account_id, binding.user_id) for binding in self.bindings]
        if len(scopes) != len(set(scopes)) or len(identities) != len(set(identities)):
            raise ValueError("memory scopes need distinct explicit OpenViking user bindings")
        return self
