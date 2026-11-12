"""Opt-in OpenViking transport; conversation scope defaults are deliberately private."""
from urllib.parse import urlsplit

from pydantic import Field, model_validator

from qqbot.domain import DTO


class OpenVikingConfig(DTO):
    endpoint: str | None = None
    api_key_env: str = "QQBOT_OPENVIKING_KEY"
    namespace: str = Field(default="elephant", pattern=r"^[a-z0-9][a-z0-9_-]{0,47}$")
    private_enabled: bool = True
    group_enabled: bool = False
    timeout_seconds: float = Field(default=2.0, ge=0.1, le=10)
    operation_budget_seconds: float = Field(default=8.0, ge=0.1, le=30)
    max_records: int = Field(default=64, ge=1, le=128)
    max_query_chars: int = Field(default=4000, ge=100, le=8000)

    @model_validator(mode="after")
    def valid_endpoint(self):
        if self.endpoint is not None:
            url = urlsplit(self.endpoint)
            if (url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password
                    or url.query or url.fragment or url.path not in {"", "/"}):
                raise ValueError("OpenViking requires an HTTP(S) origin without credentials or path")
            if url.scheme != "https" and url.hostname not in {"localhost", "127.0.0.1", "::1"}:
                raise ValueError("non-loopback OpenViking requires HTTPS")
        return self
