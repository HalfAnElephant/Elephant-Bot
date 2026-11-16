"""Interactive setup for the Elephant CLI."""
import asyncio
import base64
import fcntl
import getpass
import json
import os
import re
import secrets
import shlex
import stat
import sys
import tempfile
import warnings
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import yaml
from dotenv import dotenv_values

from elephant.config import Settings, load_runtime_settings

MODEL_KEY = "ELEPHANT_MODEL_KEY"
ADMIN_TOKEN = "ELEPHANT_ADMIN_TOKEN"
CREDENTIAL_KEY = "ELEPHANT_CREDENTIAL_KEY"
LOOPBACK = "http://127.0.0.1:8080"
ONBOARDING = "/api/v1/qq/onboarding"
DEFAULT_ENDPOINT = "https://api.deepseek.com/chat/completions"
SCAN_TIMEOUT = 180.0


def _private_write(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _private_read(path, *, system_config=False):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, encoding="utf-8") as stream:
        info = os.fstat(stream.fileno())
        mask = 0o037 if system_config else 0o077
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & mask:
            raise ValueError("setup files must be owned by the current user and protected")
        if info.st_size > 1_048_576:
            raise ValueError("setup file exceeds size limit")
        return stream.read()


def _read_if_present(path, *, system_config=False):
    try:
        return _private_read(path, system_config=system_config)
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
    finally:
        tmp.unlink(missing_ok=True)


def _credentials(raw):
    result = dotenv_values(stream=StringIO(raw or ""), interpolate=False)
    if any(not re.fullmatch("[A-Z_][A-Z_0-9]*", key) for key in result):
        raise ValueError("invalid credentials file")
    return {key: value or "" for key, value in result.items()}


def _add_credentials(raw, updates):
    # Preserve unrelated keys and text. Replace single, blank declarations only;
    # duplicate declarations would otherwise have ambiguous shell/systemd semantics.
    value = raw or ""
    for key, secret in updates.items():
        assignment = re.compile(rf"(?m)^[ \t]*(?:export[ \t]+)?{key}[ \t]*=.*$")
        hits = list(assignment.finditer(value))
        if len(hits) > 1:
            raise ValueError("duplicate credential setting")
        if any(ord(char) < 32 or ord(char) == 127 for char in secret):
            raise ValueError("invalid credential value")
        line = key + "=" + json.dumps(secret, ensure_ascii=False)
        if hits:
            value = value[:hits[0].start()] + line + value[hits[0].end():]
        else:
            value = value.rstrip("\n") + ("\n" if value else "") + line + "\n"
    return value


def _ask(label, default):
    return input(f"{label} [{default}]: ").strip() or default


def _valid_endpoint(endpoint):
    try:
        parts = urlsplit(endpoint)
        return bool(parts.hostname and parts.username is None and parts.password is None
            and not parts.query and not parts.fragment and (
                parts.scheme == "https" or parts.scheme == "http" and parts.hostname in {"127.0.0.1", "::1", "localhost"}
            ))
    except ValueError:
        return False


def _configure_model(config):
    model_config = config.setdefault("model", {})
    if not isinstance(model_config, dict):
        raise TypeError("invalid model settings")
    if not model_config.get("endpoint"):
        endpoint = _ask("模型 Chat Completions 地址", DEFAULT_ENDPOINT)
        if not _valid_endpoint(endpoint):
            raise ValueError("模型地址必须使用 HTTPS（仅本地回环可用 HTTP），且不得嵌入凭据")
        model_config["endpoint"] = endpoint
    if not model_config.get("model"):
        model_config["model"] = _ask("模型名称", "deepseek-flash")
    model_config.setdefault("api_key_env", MODEL_KEY)


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
            raise ValueError("已有 Elephant 服务占用该数据库；不会启动第二个实例") from None
        yield
    finally:
        os.close(fd)


@contextmanager
def _setup_environment(values, names):
    old = {key: os.environ.get(key) for key in names}
    try:
        for key in names:
            if values.get(key):
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


async def scan_qq(client, *, timeout=SCAN_TIMEOUT):
    """Use the same authenticated onboarding API as the setup page."""
    session_route = None
    completed = False
    provisioned = False
    try:
        async with asyncio.timeout(timeout):
            response = await client.get(ONBOARDING + "/status")
            response.raise_for_status()
            info = response.json()
            if info["adapters"]:
                print("已有 QQ 适配器，保留现有配置，不重新绑定。")
                return 0
            if not info["credential_key_ready"] or not info["automatic_available"]:
                print("QQ 扫码未就绪；保留配置，可稍后重试。")
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
                    print("QQ 扫码绑定完成，已验证本次临时连接。")
                    completed = True
                    return 0
                provisioned = provisioned or bool(session.get("provisioned"))
                if state in {"failed", "cancelled", "expired"}:
                    print("QQ 扫码未完成；已保存的配置不会被覆盖。")
                    return 1
                if not shown and state == "waiting" and session.get("connect_url"):
                    import qrcode
                    code = qrcode.QRCode(border=2)
                    code.add_data(session["connect_url"])
                    code.print_ascii(tty=False, invert=True)
                    print("请用手机 QQ 扫码并确认。")
                    shown = True
                await asyncio.sleep(1)
                response = await client.get(session_route)
                response.raise_for_status()
                session = response.json()
    except (TimeoutError, httpx.HTTPError, ValueError, KeyError):
        print("QQ 扫码未完成或已超时；配置保留，可稍后重试。")
        return 1
    finally:
        if session_route is not None and not completed and not provisioned:
            try:
                await client.delete(session_route, timeout=5)
            except httpx.HTTPError:
                pass


async def _scan_local(settings, token):
    from elephant.app import create_app
    with _exclusive_database(settings.database):
        app = create_app(settings, run_background=False)
        async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=LOOPBACK,
            headers={"Authorization": "Bearer " + token}, trust_env=False,
        ) as client:
            return await scan_qq(client)


def _new_local_configuration(directory):
    admin = {"kind": "person", "adapter_instance_id": "admin_api", "identity_namespace": "admin_api",
             "user_id": "owner"}
    task_principal = dict(admin, user_id=Settings().owner.id)
    return {
        "deployment_id": "elephant-local", "database": str(directory / "core.db"), "adapters": [],
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
                      "stream": False, "max_tokens": 24}) as response:
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


def run_setup(directory: Path | str, *, system: bool = False) -> int:
    """Generate protected setup files without overwriting existing settings."""
    directory = Path(directory).expanduser().absolute()
    try:
        if system and (directory != Path("/etc/elephant") or os.geteuid() != 0):
            raise ValueError("系统初始化须由管理员在 /etc/elephant 运行")
        if not system and os.geteuid() == 0:
            raise ValueError("本地试用请使用普通用户，不要使用 sudo")
        directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        dstat = directory.lstat()
        if (not stat.S_ISDIR(dstat.st_mode) or dstat.st_uid != os.geteuid()
                or dstat.st_mode & (0o027 if system else 0o077)):
            raise ValueError("请选择当前用户拥有的私有目录")
        cfg = directory / ("core.yaml" if system else "config.yaml")
        env = directory / ("core.env" if system else "credentials.env")
        existing_config = _read_if_present(cfg, system_config=system)
        existing_env = _read_if_present(env)
        if system and existing_config is None:
            raise ValueError("未找到受管系统配置；先安装服务")
        values = _credentials(existing_env)
        original = yaml.safe_load(existing_config) if existing_config is not None else None
        if original is None and existing_config is not None:
            original = {}
        if original is not None and not isinstance(original, dict):
            raise ValueError("invalid existing setup file")
        config = json.loads(json.dumps(original)) if original is not None else _new_local_configuration(directory)
        config.setdefault("admin_token_env", ADMIN_TOKEN)
        _configure_model(config)
        settings = Settings.model_validate_json(json.dumps(config), strict=False)
        model_key = settings.model.api_key_env
        names = (model_key, settings.admin_token_env, CREDENTIAL_KEY)
        if len(set(names)) != 3 or any(not re.fullmatch("[A-Z_][A-Z_0-9]*", key) for key in names):
            raise ValueError("invalid credential variable names")
        updates = {}
        standard_keys = (MODEL_KEY, ADMIN_TOKEN, CREDENTIAL_KEY)
        for name in names:
            if values.get(name):
                continue
            if name in standard_keys:
                alternate = "QQBOT_" + name.removeprefix("ELEPHANT_")
            elif name.startswith("QQBOT_") and "ELEPHANT_" + name.removeprefix("QQBOT_") in standard_keys:
                alternate = "ELEPHANT_" + name.removeprefix("QQBOT_")
            else:
                continue
            if values.get(alternate):
                updates[name] = values[alternate]
        if not (values.get(model_key) or updates.get(model_key)):
            with warnings.catch_warnings():
                warnings.simplefilter("error", getpass.GetPassWarning)
                secret = getpass.getpass("模型 API Key（不回显）: ").strip()
            if not re.fullmatch(r"[A-Za-z0-9._~+/=:@%#,!-]{1,8192}", secret):
                raise ValueError("模型凭据为空或含不支持的字符；未保存")
            updates[model_key] = secret
        if not (values.get(settings.admin_token_env) or updates.get(settings.admin_token_env)):
            updates[settings.admin_token_env] = secrets.token_urlsafe(32)
        if not (values.get(CREDENTIAL_KEY) or updates.get(CREDENTIAL_KEY)):
            updates[CREDENTIAL_KEY] = base64.b64encode(secrets.token_bytes(32)).decode("ascii")
        if input("向所配置的模型地址发送一次合成消息以验证联通？[y/N]: ").strip().lower() in {"y", "yes"}:
            if not asyncio.run(probe_model(settings.model, updates.get(model_key) or values[model_key])):
                print("模型验证未通过，未写入本次配置。请检查地址、模型名或 API Key 后重试。")
                return 1
            print("模型响应已验证。")
        else:
            print("未调用模型验证。")
        if updates:
            updated_env = _add_credentials(existing_env, updates)
            _replace_private(env, existing_env, updated_env)
            values.update(updates)
        if original != config:
            _replace_private(cfg, existing_config, json.dumps(config, ensure_ascii=False, indent=2) + "\n",
                             system_config=system)
        print("模型配置已保留或完成。")
        outcome = 0
        if system:
            from elephant.setup_system import scan_as_service_account, start_installed_service

            if input("现在通过官方 QQ 扫码接入？[y/N]: ").strip().lower() in {"y", "yes"}:
                outcome = max(outcome, int(scan_as_service_account(settings, values) != 0))
            if not outcome and input("现在启动服务并设为开机启动？[y/N]: ").strip().lower() in {"y", "yes"}:
                outcome = int(start_installed_service() != 0)
                if not outcome:
                    print("已请求 systemd 启动；使用 systemctl status elephant.service 检查。")
            else:
                print("未启动服务。稍后运行 sudo systemctl enable --now elephant.service。")
        elif input("现在通过官方 QQ 扫码接入？[y/N]: ").strip().lower() in {"y", "yes"}:
            token = values.get(settings.admin_token_env, "")
            if not token:
                raise ValueError("受保护的凭据文件缺少管理员令牌")
            resolved = load_runtime_settings(settings)
            if resolved.database != settings.database or resolved.admin_token_env != settings.admin_token_env:
                raise ValueError("运行时配置更改了数据库或管理凭据来源；本次不会启动临时实例")
            if resolved.adapters:
                print("已有 QQ 适配器，保留现有配置，不重新绑定。")
            else:
                with _setup_environment(values, names):
                    outcome = max(outcome, asyncio.run(_scan_local(settings, token)))
        if not system:
            print(f"启动：uv run elephant serve --config {shlex.quote(str(cfg))} --env-file {shlex.quote(str(env))}")
        return outcome
    except (OSError, TypeError, ValueError, EOFError, getpass.GetPassWarning, yaml.YAMLError):
        print("初始化未完成。已保存的设置可保留；请检查私有目录、模型配置或终端输入。", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\n已取消初始化。", file=sys.stderr)
        return 130
