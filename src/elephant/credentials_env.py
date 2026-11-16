"""Read supported credential variables without changing the original environment."""

import os

_SUFFIXES = (
    "ADMIN_TOKEN", "CREDENTIAL_KEY", "GITHUB_TOKEN", "INTERNAL_TOKEN", "MODEL_KEY",
    "OFFICIAL_SECRET", "OPENVIKING_KEY", "RUNNER_OPENAI_KEY",
)
_ALIASES = {f"ELEPHANT_{suffix}": f"QQBOT_{suffix}" for suffix in _SUFFIXES}
_ALIASES.update({old: new for new, old in tuple(_ALIASES.items())})


def credential_env(name: str, default: str = "") -> str:
    """Prefer the configured name, including an explicitly blank value."""
    if name in os.environ:
        return os.environ[name]
    alternate = _ALIASES.get(name)
    return os.environ.get(alternate, default) if alternate else default
