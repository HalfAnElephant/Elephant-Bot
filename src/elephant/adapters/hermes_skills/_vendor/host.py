"""Elephant-owned profile and filesystem bindings for the vendored Hermes tools."""
import contextvars
import json
import os
import tempfile
from contextlib import contextmanager
from enum import Enum
from pathlib import Path

_home = contextvars.ContextVar('hermes_owner_home', default=None)
_options = contextvars.ContextVar('hermes_owner_options', default=None)


def get_hermes_home():
    # Upstream defines import-time path constants; writes always use the live binding.
    return _home.get() or Path('/__elephant_unbound_hermes_owner__')


@contextmanager
def bind_home(directory, options=None):
    token = _home.set(Path(directory))
    configured = _options.set(options or {})
    try:
        yield
    finally:
        _options.reset(configured)
        _home.reset(token)


def get_skills_dir():
    return get_hermes_home() / 'skills'


def get_config_path():
    return get_hermes_home() / 'config.yaml'


def get_subprocess_home():
    return str(get_hermes_home())


def cfg_get(config, *keys, default=None):
    for key in keys:
        if not isinstance(config, dict) or key not in config:
            return default
        config = config[key]
    return config


def load_config():
    return {'skills': {'create_dir': str(get_skills_dir()), 'write_approval': False,
                       'inline_shell': False, 'ledger': True, 'guard_agent_created': False, **(_options.get() or {})}}


load_config_readonly = load_config


def is_truthy_value(value, default=False):
    return default if value is None else value is True or str(value).lower() in {'true', '1', 'yes', 'on'}


def mkdir_under_hermes_home(path):
    path = Path(path)
    if _home.get() is None or not path.resolve().is_relative_to(get_hermes_home()):
        raise PermissionError('skill path is outside the bound owner root')
    path.mkdir(parents=True, exist_ok=True, mode=0o700)


def atomic_write_text(path, content, *, preserve_mode=False, create_mode=0o600, tmp_prefix=".skill_"):
    path = Path(path)
    mkdir_under_hermes_home(path.parent)
    mode = path.stat().st_mode & 0o777 if preserve_mode and path.exists() else create_mode
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, prefix=tmp_prefix, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(mode)
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


class SkillReadinessStatus(str, Enum):
    UNSUPPORTED = 'unsupported'


def tool_error(message, **extra):
    return json.dumps({'error': str(message), **extra}, ensure_ascii=False)
