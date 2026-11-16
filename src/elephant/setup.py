"""Interactive setup for the Elephant CLI."""
import asyncio
import base64
import fcntl
import getpass
import json
import os
import platform
import re
import secrets
import stat
import tempfile
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import yaml
from alembic.util import CommandError
from dotenv.parser import parse_stream
from sqlalchemy.exc import SQLAlchemyError

from elephant.config import Settings, load_runtime_settings, overlay_runtime_settings, runtime_config_path
from elephant.configuration import ConfigurationStore, DurabilityError, sync_directory
from elephant.providers import output_limit_parameter
from elephant.setup_providers import MODEL_PROVIDERS
from elephant.setup_ui import SetupUI

MODEL_KEY = "ELEPHANT_MODEL_KEY"
ADMIN_TOKEN = "ELEPHANT_ADMIN_TOKEN"
CREDENTIAL_KEY = "ELEPHANT_CREDENTIAL_KEY"
LOOPBACK = "http://127.0.0.1:8080"
ONBOARDING = "/api/v1/qq/onboarding"
SCAN_TIMEOUT = 180.0


def _private_write(path, data):
    fd, name = tempfile.mkstemp(prefix=".setup-", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        # Publish the complete file without overwriting an unexpected destination.
        os.link(tmp, path)
        tmp.unlink()
        sync_directory(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def _private_read(path, *, system_config=False, allowed_uids=None):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        mask = 0o037 if system_config else 0o077
        owners = (os.geteuid(),) if allowed_uids is None else allowed_uids
        if not stat.S_ISREG(info.st_mode) or info.st_uid not in owners or info.st_mode & mask:
            raise ValueError("setup files must be owned by the current user and protected")
        if info.st_size > 1_048_576:
            raise ValueError("setup file exceeds size limit")
        return stream.read()


def _read_if_present(path, *, system_config=False, allowed_uids=None):
    try:
        return _private_read(path, system_config=system_config, allowed_uids=allowed_uids)
    except FileNotFoundError:
        return None


def _replace_private(path, prior, updated, *, system_config=False):
    if prior is None:
        _private_write(path, updated)
        return
    if _private_read(path, system_config=system_config) != prior:
        raise ValueError("setup files changed during initialization")
    info = path.lstat()
    fd, name = tempfile.mkstemp(prefix=".setup-", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            if os.geteuid() == 0:
                os.fchown(stream.fileno(), info.st_uid, info.st_gid)
            os.fchmod(stream.fileno(), stat.S_IMODE(info.st_mode))
            stream.write(updated)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        sync_directory(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def _credential_bindings(raw):
    bindings = list(parse_stream(StringIO(raw or "")))
    if any(binding.error or binding.key is not None and not re.fullmatch(
        "[A-Z_][A-Z_0-9]*", binding.key
    ) for binding in bindings):
        raise ValueError("invalid credentials file")
    return bindings


def _credentials(raw):
    return {binding.key: binding.value or "" for binding in _credential_bindings(raw) if binding.key is not None}


def _add_credentials(raw, updates):
    original = _credentials(raw)
    pending = dict(updates)
    seen = set()
    parts = []
    for binding in _credential_bindings(raw):
        key = binding.key
        if key not in updates:
            parts.append(binding.original.string)
            continue
        if key in seen:
            raise ValueError("duplicate credential setting")
        seen.add(key)
        leading = re.match(r"\s*", binding.original.string).group()
        parts.append(leading + _credential_line(key, pending.pop(key)))
    updated = "".join(parts)
    if pending:
        if updated and not updated.endswith("\n"):
            updated += "\n"
        updated += "".join(_credential_line(key, secret) for key, secret in pending.items())
    if _credentials(updated) != {**original, **updates}:
        raise ValueError("credential file update failed validation")
    return updated


def _credential_line(key, value):
    if not re.fullmatch("[A-Z_][A-Z_0-9]*", key) or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("invalid credential name or value")
    return key + "=" + json.dumps(value, ensure_ascii=False) + "\n"


def _validate_credential_roles(settings):
    names = (settings.model.api_key_env, settings.admin_token_env, CREDENTIAL_KEY)
    if any(not re.fullmatch("[A-Z_][A-Z_0-9]*", name) for name in names):
        raise ValueError("invalid credential variable names")
    normalized = tuple("ELEPHANT_" + name[6:] if name.startswith("QQBOT_") else name for name in names)
    reserved = {MODEL_KEY, ADMIN_TOKEN, CREDENTIAL_KEY, "ELEPHANT_GITHUB_TOKEN", "ELEPHANT_INTERNAL_TOKEN",
                "ELEPHANT_OFFICIAL_SECRET", "ELEPHANT_OPENVIKING_KEY", "ELEPHANT_RUNNER_OPENAI_KEY"}
    role_names = (MODEL_KEY, ADMIN_TOKEN, CREDENTIAL_KEY)
    if len(set(normalized)) != 3 or any(
        (name in reserved and name != own)
        or (name.startswith(MODEL_KEY + "_") and own != MODEL_KEY)
        for name, own in zip(normalized, role_names)
    ):
        raise ValueError("credential variable is assigned to the wrong role")
    return names


def _valid_endpoint(endpoint):
    try:
        if not endpoint or any(char.isspace() or ord(char) < 32 or char in "\\?#" for char in endpoint):
            return False
        parts = urlsplit(endpoint)
        _ = parts.port
        return bool(parts.hostname and parts.username is None and parts.password is None and (
            parts.scheme == "https" or parts.scheme == "http" and parts.hostname in {"127.0.0.1", "::1", "localhost"}
        ))
    except ValueError:
        return False


def _custom_endpoint(ui):
    while True:
        endpoint = ui.ask("服务地址")
        if _valid_endpoint(endpoint):
            endpoint = endpoint.rstrip("/")
            return endpoint if urlsplit(endpoint).path.endswith("/chat/completions") else endpoint + "/chat/completions"
        ui.status("warn", "请输入 HTTPS 地址。本机服务可以使用 HTTP；地址中不要包含密钥。")


def _custom_model(ui):
    while True:
        name = ui.ask("模型名称")
        if re.fullmatch(r"[A-Za-z0-9_.:/@+-]{1,200}", name):
            return name
        ui.status("warn", "请填写服务商提供的模型名称。")


def _model_label(endpoint):
    return next((provider.label for provider in MODEL_PROVIDERS if provider.endpoint == endpoint), "自定义服务")


def _pick_model(ui, model, *, replace=False):
    """Fill gaps in existing settings; never silently change an existing destination."""
    if model.get("endpoint") and not replace:
        if not model.get("model"):
            model["model"] = _custom_model(ui)
        return
    providers = [provider.label for provider in MODEL_PROVIDERS]
    default = next((i for i, entry in enumerate(MODEL_PROVIDERS)
                    if entry.endpoint == model.get("endpoint")), 0)
    while True:
        choice = ui.choose("你想使用哪个模型服务？", [*providers, "自定义服务 · 兼容接口"], default=default)
        if choice == len(providers):
            ui.section("自定义服务", "只需填写服务商提供的地址和模型名称。")
            endpoint, name = _custom_endpoint(ui), model.get("model") if not replace else None
            name = name or _custom_model(ui)
        else:
            provider = MODEL_PROVIDERS[choice]
            endpoint = provider.endpoint
            name = model.get("model") if not replace else None
            if not name:
                options = [provider.models[0] + "（推荐）", *provider.models[1:], "填写其他模型", "返回服务列表"]
                idx = ui.choose("选择模型", options)
                if idx == len(options) - 1:
                    default = choice
                    continue
                name = provider.models[idx] if idx < len(provider.models) else _custom_model(ui)
        model.update(endpoint=endpoint, model=name)
        return


def _enter_key(ui):
    while True:
        secret = ui.ask("输入 API 密钥", secret=True).strip()
        if re.fullmatch(r"[A-Za-z0-9._~+/=:@%#,!-]{1,8192}", secret):
            return secret
        ui.status("warn", "密钥为空或格式不支持，请重新粘贴。")


def _collect_credentials(values, names):
    updates = {}
    standard_keys = (MODEL_KEY, ADMIN_TOKEN, CREDENTIAL_KEY)
    for name in names:
        if name in values:
            continue
        if name in standard_keys:
            alternate = "QQBOT_" + name.removeprefix("ELEPHANT_")
        elif name.startswith("QQBOT_") and "ELEPHANT_" + name.removeprefix("QQBOT_") in standard_keys:
            alternate = "ELEPHANT_" + name.removeprefix("QQBOT_")
        else:
            continue
        if values.get(alternate):
            updates[name] = values[alternate]
    return updates


class _DatabaseBusy(ValueError):
    pass


@contextmanager
def _exclusive_database(path):
    database = Path(path)
    database.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(str(database) + ".core.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("数据库锁不是普通文件")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise _DatabaseBusy("已有 Elephant 服务占用该数据库；不会启动第二个实例") from None
        yield
    finally:
        os.close(fd)


@contextmanager
def _setup_environment(values, names):
    old = {key: os.environ.get(key) for key in names}
    try:
        for key in names:
            if key in values:
                os.environ[key] = values[key]
            else:
                os.environ.pop(key, None)
        yield
    finally:
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


async def scan_qq(client, *, timeout=SCAN_TIMEOUT, ui=None):
    """Use the same authenticated onboarding API as the setup page."""
    ui = SetupUI() if ui is None else ui
    session_route = None
    completed = False
    provisioned = False
    try:
        async with asyncio.timeout(timeout):
            response = await client.get(ONBOARDING + "/status")
            response.raise_for_status()
            info = response.json()
            if info["adapters"]:
                ui.status("ok", "已配置 QQ，保留原有连接。")
                return 0
            if not info["credential_key_ready"] or not info["automatic_available"]:
                ui.status("warn", "目前无法生成二维码，可以稍后重试。")
                return 1
            response = await client.post(
                ONBOARDING + "/sessions", json={"instance_id": "qq-official", "automatic": True})
            response.raise_for_status()
            session = response.json()
            identifier = session["session_id"]
            if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", identifier):
                raise ValueError("invalid QR session")
            session_route = ONBOARDING + "/sessions/" + identifier
            shown = False
            while True:
                state = session["state"]
                if state == "connected":
                    ui.status("ok", "已完成扫码，并验证本次 QQ 连接。")
                    completed = True
                    return 0
                provisioned = provisioned or bool(session.get("provisioned"))
                if state in {"failed", "cancelled", "expired"}:
                    ui.status("warn", "未完成扫码，已保存的设置不受影响。")
                    return 1
                if not shown and state == "waiting" and session.get("connect_url"):
                    import qrcode
                    code = qrcode.QRCode(border=2)
                    code.add_data(session["connect_url"])
                    code.print_ascii(tty=False, invert=True)
                    ui.status("info", "请用手机 QQ 扫码并确认。")
                    shown = True
                await asyncio.sleep(1)
                response = await client.get(session_route)
                response.raise_for_status()
                session = response.json()
    except (TimeoutError, httpx.HTTPError, ValueError, KeyError):
        ui.status("warn", "未完成扫码，请稍后重试。")
        return 1
    finally:
        if session_route is not None and not completed and not provisioned:
            try:
                await client.delete(session_route, timeout=5)
            except httpx.HTTPError:
                pass


async def _scan_local(settings, token, *, ui=None):
    from elephant.app import create_app
    with _exclusive_database(settings.database):
        app = create_app(settings, run_background=False)
        async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=LOOPBACK,
            headers={"Authorization": "Bearer " + token}, trust_env=False,
        ) as client:
            return await scan_qq(client, ui=ui)


def _new_local_configuration(directory):
    admin = {"kind": "person", "adapter_instance_id": "admin_api", "identity_namespace": "admin_api",
             "user_id": "owner"}
    task_principal = dict(admin, user_id=Settings().owner.id)
    return {
        "deployment_id": "elephant-local", "database": str(directory / "core.db"),
        "internal_socket": str(directory / "internal.sock"), "adapters": [],
        "tasks_enabled": True, "execution": {"leader_environment": "sandbox", "worker_environment": "sandbox"},
        "permissions": {"preset": "custom", "default_effect": "deny", "rules": [
            {"id": "setup-admin-console", "subject": admin, "actions": ["admin.*"],
             "resources": ["http:/api/v1/*"], "effect": "allow"},
            {"id": "setup-admin-policy", "subject": admin, "actions": ["permission.list", "permission.set_policy"],
             "resources": ["permission:policy"], "effect": "allow"},
            {"id": "setup-admin-read-task", "subject": admin, "actions": ["task.read"],
             "resources": ["task:*"], "effect": "allow"},
            {"id": "setup-admin-service-status", "subject": admin, "actions": ["service.status.read"],
             "resources": ["service:core"], "effect": "allow"},
            {"id": "setup-admin-first-task", "subject": task_principal, "actions": ["task.*", "model.request"],
             "resources": ["*"], "effect": "allow"},
            {"id": "setup-admin-first-task-source", "subject": task_principal, "actions": ["history.source.read"],
             "resources": ["event:*"], "effect": "allow"},
            {"id": "setup-admin-first-task-context", "subject": task_principal,
             "actions": ["history.read", "notification.send"],
             "resources": ["conversation:*"], "effect": "allow"},
        ]},
    }


async def probe_model(model, key, *, transport=None):
    """Send only a fixed readiness prompt, never forwarded dialogue or tools."""
    if not _valid_endpoint(model.endpoint):
        return False
    try:
        async with asyncio.timeout(15), httpx.AsyncClient(transport=transport, timeout=10,
                trust_env=False, follow_redirects=False) as client, client.stream("POST", model.endpoint,
                headers={"Authorization": "Bearer " + key},
                json={"model": model.model, "messages": [{"role": "user", "content": "Reply with OK."}],
                      "stream": False, output_limit_parameter(model.endpoint): 512}) as response:
            response.raise_for_status()
            data = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=8192):
                data.extend(chunk)
                if len(data) > 65536:
                    return False
            content = json.loads(data)["choices"][0]["message"]["content"]
            return isinstance(content, str) and bool(content.strip())
    except (httpx.HTTPError, TimeoutError, ValueError, KeyError, IndexError, TypeError):
        return False


def _load_wizard_model(ui, initial, values):
    """Choose and optionally verify model settings before any persistent write."""
    persisted = initial.get("model") or {}
    model = dict(persisted)
    action = 0
    if model.get("endpoint") and model.get("model"):
        ui.status("info", f"当前使用 {_model_label(model['endpoint'])}。")
        action = ui.choose("如何处理当前模型设置？", ("保留当前设置", "更换模型服务或模型", "更换当前模型密钥"))
    _pick_model(ui, model, replace=action == 1)

    # A key may only be reused for its existing endpoint. A new slot keeps
    # interrupted writes safe and leaves older credentials available for rollback.
    old_key = persisted.get("api_key_env", MODEL_KEY)
    old_keys = {old_key, MODEL_KEY, "QQBOT_MODEL_KEY"}
    if action == 2 or (model["endpoint"] != persisted.get("endpoint") and (
        persisted.get("endpoint") or any(values.get(name) for name in old_keys)
    )):
        model["api_key_env"] = "ELEPHANT_MODEL_KEY_" + secrets.token_hex(8).upper()
    model.setdefault("api_key_env", MODEL_KEY)

    force_key = action == 2
    while True:
        candidate = {**initial, "model": model}
        settings = Settings.model_validate_json(json.dumps(candidate), strict=False)
        model_key = settings.model.api_key_env
        names = _validate_credential_roles(settings)
        updates = _collect_credentials(values, names)
        if force_key or not (values.get(model_key) or updates.get(model_key)):
            updates[model_key] = _enter_key(ui)
        else:
            ui.status("ok", "沿用已有密钥。")

        ui.section("2/4  连接检测", "可发送一条简短的测试消息，不会发送聊天记录。")
        if not ui.confirm("现在检测模型连接？"):
            ui.status("info", "已跳过检测，可稍后回来验证。")
            return candidate, settings, updates, False
        with ui.progress("正在等待模型响应…"):
            verified = asyncio.run(probe_model(settings.model, updates.get(model_key) or values[model_key]))
        if verified:
            ui.status("ok", "模型已响应。")
            return candidate, settings, updates, True
        ui.status("warn", "未收到可用的模型回复。当前设置未保存。")
        retry = ui.choose("接下来怎么做？", ("退出设置", "重选模型服务", "重新输入密钥"))
        if not retry:
            return None
        ui.section("1/4  模型服务", "调整后可以再试一次。")
        if retry == 1:
            before = model["endpoint"]
            _pick_model(ui, model, replace=True)
            if model["endpoint"] == persisted.get("endpoint"):
                model["api_key_env"] = old_key
            elif model["endpoint"] != before:
                model["api_key_env"] = "ELEPHANT_MODEL_KEY_" + secrets.token_hex(8).upper()
            force_key = False
        else:
            # Keep the original credential intact until the new setup is saved.
            if model_key in values or model["endpoint"] == persisted.get("endpoint"):
                model["api_key_env"] = "ELEPHANT_MODEL_KEY_" + secrets.token_hex(8).upper()
            force_key = True


def _bind_qq(ui, settings, values, *, system):
    if system:
        from elephant.setup_system import scan_as_service_account
        return scan_as_service_account(settings, values)
    token = values.get(settings.admin_token_env, "")
    resolved = load_runtime_settings(settings)
    if (not token or resolved.database != settings.database
            or resolved.admin_token_env != settings.admin_token_env):
        ui.status("warn", "当前设置不支持临时扫码，可通过正在运行的管理页完成。")
        return 1
    if resolved.adapters:
        ui.status("ok", "已配置 QQ，保留原有连接。")
        return 0
    names = (resolved.model.api_key_env, settings.admin_token_env, CREDENTIAL_KEY)
    with _setup_environment(values, names):
        return asyncio.run(_scan_local(settings, token, ui=ui))


def _start_local(settings, values, *, on_ready):
    from elephant.cli import serve_local
    with _setup_environment(values, values):
        serve_local(settings, load_runtime_settings(settings), on_ready=on_ready)


@contextmanager
def _configuration_lock(directory):
    fd = os.open(directory / ".setup.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise ValueError("invalid setup lock")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def _read_setup_overrides(startup, *, system):
    path = runtime_config_path(startup)
    owners = None
    if system:
        import pwd

        from elephant.setup_system import ACCOUNT
        try:
            owners = (os.geteuid(), pwd.getpwnam(ACCOUNT).pw_uid)
        except KeyError as exc:
            raise ValueError("installer-managed service account is missing") from exc
    raw = _read_if_present(path, allowed_uids=owners)
    return path, raw, json.loads(raw) if raw is not None else {}


def _persist_setup_files(cfg, env, *, existing_config, config, original, existing_env, values, updates, system):
    if updates:
        updated_env = _add_credentials(existing_env, updates)
        _replace_private(env, existing_env, updated_env)
        values.update(updates)
    if original != config:
        _replace_private(cfg, existing_config, json.dumps(config, ensure_ascii=False, indent=2) + "\n",
                         system_config=system)


def _stored_encrypted_credentials(startup, runtime):
    if any(adapter.credential_ref for adapter in (*startup.adapters, *runtime.adapters)):
        return True
    databases = (Path(startup.database), Path(runtime.database))
    roots = {root for path in databases for root in (path.absolute().parent, path.resolve().parent)}
    for root in roots:
        for name in ("qq-credentials", "mail-credentials"):
            try:
                if next((root / name).iterdir(), None) is not None:
                    return True
            except FileNotFoundError:
                continue
    return False


def _vault_can_resume(ui, startup, runtime, values):
    # Explicitly blank canonical keys disable alias fallback.
    key = values.get(CREDENTIAL_KEY, values.get("QQBOT_CREDENTIAL_KEY", ""))
    if key:
        try:
            if len(base64.b64decode(key, validate=True)) == 32:
                return True
        except ValueError:
            pass
    elif not _stored_encrypted_credentials(startup, runtime):
        return True
    ui.status("error", "凭据加密密钥缺失或无效。请从备份恢复原密钥后重开向导；不会生成新密钥替换它。")
    return False


def _prepare_configuration(ui, directory, *, system):
    with _configuration_lock(directory):
        cfg = directory / ("core.yaml" if system else "config.yaml")
        env = directory / ("core.env" if system else "credentials.env")
        existing_config = _read_if_present(cfg, system_config=system)
        existing_env = _read_if_present(env)
        if system and existing_config is None:
            ui.status("error", "未找到受管安装，请先完成 Linux 系统安装。")
            return None
        values = _credentials(existing_env)
        original = yaml.safe_load(existing_config) if existing_config is not None else None
        if original is None and existing_config is not None:
            original = {}
        if original is not None and not isinstance(original, dict):
            raise ValueError("invalid existing setup file")
        config = dict(original) if original is not None else _new_local_configuration(directory)
        config.setdefault("admin_token_env", ADMIN_TOKEN)
        startup = Settings.model_validate_json(json.dumps(config), strict=False)
        _validate_credential_roles(startup)
        _, saved_snapshot, overrides = _read_setup_overrides(startup, system=system)
        runtime = overlay_runtime_settings(startup, overrides)
        _validate_credential_roles(runtime)
        if not _vault_can_resume(ui, startup, runtime, values):
            return None
        snapshot_owns_model = "model" in overrides
        edit_model = (runtime.admin_token_env == startup.admin_token_env
                      and (not system or not snapshot_owns_model))

        ui.banner()
        ui.section("1/4  模型服务", "从常用服务中选择，地址会自动配置。")
        settings, effective = startup, runtime
        if not edit_model:
            ui.status("warn", "当前模型由管理页维护；请在管理页更换。向导不会覆盖设置。")
            verified = False
            updates = _collect_credentials(values, (startup.admin_token_env, CREDENTIAL_KEY))
            ui.section("2/4  连接检测", "已保留当前模型，不重新验证。")
        else:
            # Presence, not equality, determines which file owns the model.
            initial = config if not snapshot_owns_model else {**config, "model": runtime.model.model_dump(mode="json")}
            selection = _load_wizard_model(ui, initial, values)
            if selection is None:
                return None
            chosen_config, chosen, updates, verified = selection
            if snapshot_owns_model:
                effective = overlay_runtime_settings(startup, {**overrides, "model": chosen.model.model_dump(mode="json")})
            else:
                config, settings = chosen_config, chosen
                effective = overlay_runtime_settings(settings, overrides)
        if not (values.get(settings.admin_token_env) or updates.get(settings.admin_token_env)):
            updates[settings.admin_token_env] = secrets.token_urlsafe(32)
        if not (values.get(CREDENTIAL_KEY) or updates.get(CREDENTIAL_KEY)):
            updates[CREDENTIAL_KEY] = base64.b64encode(secrets.token_bytes(32)).decode("ascii")

        changed_model = effective.model != runtime.model
        if updates or original != config or changed_model:
            if not system and runtime.database != startup.database:
                # A saved database move may leave a live daemon on the old
                # database; holding just the new lock cannot fence it.
                ui.status("warn", "检测到管理页保存了不同的数据库位置。请先在管理页处理，向导不会改写当前配置。")
                return None
            try:
                if system:
                    from elephant.setup_system import system_configuration_lock

                    lock = system_configuration_lock(runtime)
                else:
                    lock = _exclusive_database(runtime.database)
                with lock:
                    if _read_setup_overrides(startup, system=system)[1] != saved_snapshot:
                        ui.status("warn", "模型设置刚发生变化，未覆盖。请重新打开向导。")
                        return None
                    # Recheck the old key and vault after fencing the daemon,
                    # before writes in case credentials were stored during a prompt.
                    if not _vault_can_resume(ui, startup, runtime, values):
                        return None
                    _persist_setup_files(
                        cfg, env, existing_config=existing_config, config=config, original=original,
                        existing_env=existing_env, values=values, updates=updates, system=system,
                    )
                    store = ConfigurationStore(settings)
                    if snapshot_owns_model and changed_model:
                        store.save_model_while_stopped(runtime, effective, overrides)
                    if store.read().model != effective.model:
                        ui.status("error", "未能确认模型设置已保存，请检查管理页。")
                        return None
            except _DatabaseBusy:
                ui.status("warn", "Elephant 正在运行。请先停止服务再修改模型或凭据；向导不会自动重启。")
                return None
        ui.status("ok", "已保存设置，密钥不会显示在屏幕上。")
        return settings, values, verified, _model_label(effective.model.endpoint)


def run_setup(directory: Path | str, *, system: bool = False) -> int:
    """Guide setup while retaining protected writes and explicit side effects."""
    directory = Path(directory).expanduser().absolute()
    ui = SetupUI()
    try:
        if system and (directory != Path("/etc/elephant") or os.geteuid() != 0):
            raise ValueError("system setup requires the managed installation")
        if not system and os.geteuid() == 0:
            ui.status("error", "本地试用请用普通用户打开向导。")
            return 1
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        dstat = directory.lstat()
        if (not stat.S_ISDIR(dstat.st_mode) or dstat.st_uid != os.geteuid()
                or dstat.st_mode & (0o027 if system else 0o077)):
            raise ValueError("setup requires a private directory")
        prepared = _prepare_configuration(ui, directory, system=system)
        if prepared is None:
            return 1
        settings, values, verified, model_label = prepared

        ui.section("3/4  连接 QQ", "接入后，可以直接用手机和 Elephant 对话。")
        qq = "未检测"
        outcome = 0
        if ui.confirm("现在用手机 QQ 扫码？"):
            try:
                with ui.progress("正在准备 QQ 二维码…"):
                    outcome = _bind_qq(ui, settings, values, system=system)
            except (OSError, ValueError, RuntimeError, ImportError, CommandError, SQLAlchemyError):
                ui.status("warn", "现在无法扫码。如果服务已在运行，可从管理页连接。")
                outcome = 1
            qq = "已配置" if not outcome else "待完成"
        else:
            ui.status("info", "已跳过，之后可以重新打开向导接入。")

        ui.section("4/4  开始使用", "按需要启动，也可以先退出，已保存的设置不会丢失。")
        macos = not system and platform.system() == "Darwin"
        if system and not outcome:
            choices = ("稍后启动", "现在启动，并设为开机启动")
        elif system:
            choices = ("稍后启动",)
        elif macos:
            choices = ("稍后启动", "在当前窗口启动", "现在启动，并在登录 Mac 时自动运行")
        else:
            choices = ("稍后启动", "在当前窗口启动")
        start = ui.choose("启动方式", choices)
        status = "已通过检测" if verified else "未检测"
        ui.section("设置已保存", f"模型：{model_label}"
                   f" · {status}    |    QQ：{qq}")
        if start:
            if system:
                from elephant.setup_system import start_installed_service
                with ui.progress("正在启动系统服务…"):
                    result = start_installed_service(quiet=True)
                if result:
                    ui.status("error", "未能启动系统服务。设置仍已保存，请查看部署文档的排查说明。")
                    return 1
                ui.status("ok", "已提交启动请求，并设为开机启动；不会重启已运行的服务。")
            elif macos and start == 2:
                from elephant.setup_macos import LaunchAgentError, install_launch_agent
                try:
                    with ui.progress("正在设置登录后自动运行…"):
                        loaded = install_launch_agent(
                            directory, pi_executable=load_runtime_settings(settings).orchestration.pi_executable,
                        )
                except LaunchAgentError as error:
                    ui.status("warn", str(error))
                    return 1
                if loaded:
                    ui.status("ok", "已设置登录后自动运行，已向 macOS 提交启动请求。")
                else:
                    ui.status("ok", "已有相同的登录启动项，未重新启动。")
            else:
                ui.status("info", "正在启动，请保持当前窗口打开。按 Ctrl+C 可停止。")
                def on_ready():
                    tip = "现在可以去 QQ 发一条消息。" if qq == "已配置" else ""
                    ui.status("ok", "Elephant 已运行。" + tip)

                try:
                    _start_local(settings, values, on_ready=on_ready)
                except CommandError:
                    ui.status("error", "未能启动。请从 Elephant 安装目录打开向导。")
                    return 1
                except (OSError, RuntimeError, ImportError, SQLAlchemyError, SystemExit):
                    ui.status("error", "未能启动。请确认没有另一个 Elephant 实例，或查阅部署文档。")
                    return 1
        else:
            ui.status("info", "准备好后，重新打开向导即可启动。")
        return int(outcome != 0)
    except DurabilityError:
        ui.status("error", "文件可能已更新，但无法确认已写入磁盘。请检查磁盘，然后重新打开向导。")
        return 1
    except (OSError, TypeError, ValueError, RuntimeError, EOFError, getpass.GetPassWarning, yaml.YAMLError):
        ui.status("error", "设置未完成。请检查安装环境，或从部署文档查看排查说明。")
        return 1
    except KeyboardInterrupt:
        ui.status("info", "已退出向导。先前保存的设置仍会保留。")
        return 130
