"""Exercise the installed systemd service only in an empty GitHub-hosted VM."""
import http.server
import importlib.util
import json
import os
import secrets
import socket
import stat
import subprocess
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

import yaml
from dotenv import dotenv_values

from elephant.cli import SERVE_PORT
from elephant.config import load_settings, runtime_config_path
from elephant.setup_system import CONFIG, ROOT, STATE, UNIT

ENVIRONMENT = CONFIG.with_suffix(".env")
DATABASE = STATE / "core.db"
MODEL_KEY_ENV = "ELEPHANT_SYSTEMD_SMOKE_MODEL_KEY"
LOOPBACK = "127.0.0.1"
SYSTEM_ENV = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8"}
STAGE = "preflight"


def systemctl(operation, *arguments):
    result = subprocess.run(["/usr/bin/systemctl", operation, *arguments, "elephant.service"],
        env=SYSTEM_ENV, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=45, check=False)
    if result.returncode:
        raise RuntimeError("systemd smoke service operation failed")
    return result.stdout.strip()


def saved_file(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
        raise RuntimeError("systemd smoke requires trusted installed files")
    return path.read_bytes(), info


def publish_file(path, content, info):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            os.fchown(handle.fileno(), info.st_uid, info.st_gid)
            os.fchmod(handle.fileno(), stat.S_IMODE(info.st_mode))
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def database_rows(query):
    # Even a read-only WAL reader may create shared-memory files; use the service owner.
    source = ("import json,sqlite3,sys; "
              "db=sqlite3.connect('file:/var/lib/elephant/core.db?mode=ro',uri=True); "
              "print(json.dumps(db.execute(sys.argv[1]).fetchall())); db.close()")
    result = subprocess.run(["/usr/sbin/runuser", "-u", "elephant", "-g", "elephant-worker", "--",
        str(ROOT / ".venv/bin/python"), "-I", "-B", "-c", source, query], env=SYSTEM_ENV,
        stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=10, check=False)
    if result.returncode:
        raise RuntimeError("systemd smoke database read failed")
    return json.loads(result.stdout)


def require_empty_installation():
    if os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted" or os.geteuid() != 0:
        raise RuntimeError("systemd smoke requires a disposable GitHub-hosted root process")
    if Path(__file__).absolute() != ROOT / "scripts/smoke_systemd_install.py":
        raise RuntimeError("systemd smoke requires the standard installed release")
    for path in (Path(__file__), ROOT / "scripts/smoke_guided_install.py", UNIT):
        saved_file(path)
    if systemctl("show", "--property=FragmentPath", "--value") != str(UNIT):
        raise RuntimeError("systemd smoke requires the installed unit")
    if systemctl("show", "--property=ActiveState", "--value") != "inactive":
        raise RuntimeError("systemd smoke requires an inactive service")
    settings = load_settings(CONFIG)
    if (settings.database != str(DATABASE) or DATABASE.resolve() != DATABASE
            or settings.adapters or settings.memory.openviking.endpoint
            or runtime_config_path(settings).exists()):
        raise RuntimeError("systemd smoke requires a fresh local-only configuration")
    tables = ("tasks", "inbox", "outbox", "conversations", "mail_accounts", "schedules")
    if database_rows("SELECT " + " OR ".join(f"EXISTS(SELECT 1 FROM {table})" for table in tables))[0][0]:
        raise RuntimeError("systemd smoke refuses an installation containing user data")
    with socket.socket() as probe:
        probe.bind((LOOPBACK, SERVE_PORT))
    return settings


def request(opener, token, path, payload=None):
    headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json",
               "Idempotency-Key": "systemd-smoke-first-task"}
    body = json.dumps(payload).encode() if payload is not None else None
    query = urllib.request.Request(f"http://{LOOPBACK}:{SERVE_PORT}" + path, body, headers)
    with opener.open(query, timeout=2) as response:
        return json.load(response)


def wait_ready(opener, token):
    deadline = time.monotonic() + 40
    while time.monotonic() < deadline:
        try:
            if request(opener, token, "/health/ready")["ready"]:
                return
        except OSError:
            pass
        time.sleep(.2)
    raise RuntimeError("systemd smoke readiness timed out")


def run():
    global STAGE
    STAGE = "preflight"
    settings = require_empty_installation()
    original = {path: saved_file(path) for path in (CONFIG, ENVIRONMENT)}
    values = dotenv_values(ENVIRONMENT, interpolate=False)
    token = values.get(settings.admin_token_env) or secrets.token_urlsafe(32)
    spec = importlib.util.spec_from_file_location("guided_smoke", ROOT / "scripts/smoke_guided_install.py")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), helper.NoRedirect())
    with http.server.ThreadingHTTPServer((LOOPBACK, 0), helper.ModelHandler) as model:
        thread = threading.Thread(target=model.serve_forever, daemon=True)
        thread.start()
        try:
            candidate = settings.model_dump(mode="json")
            candidate["model"] = {**candidate["model"], "endpoint": f"http://{LOOPBACK}:{model.server_port}/chat/completions",
                                  "model": helper.MODEL, "api_key_env": MODEL_KEY_ENV}
            environment = {settings.admin_token_env: token, MODEL_KEY_ENV: helper.MODEL_KEY,
                           "HTTP_PROXY": "", "HTTPS_PROXY": "", "ALL_PROXY": "", "NO_PROXY": LOOPBACK + ",localhost"}
            started = time.monotonic()
            try:
                STAGE = "config"
                publish_file(CONFIG, yaml.safe_dump(candidate).encode(), original[CONFIG][1])
                content = "".join(name + "=" + json.dumps(value) + "\n" for name, value in environment.items())
                publish_file(ENVIRONMENT, content.encode(), original[ENVIRONMENT][1])
                STAGE = "start"
                systemctl("start")
                wait_ready(opener, token)
                ready_seconds = time.monotonic() - started
                STAGE = "first_task"
                goal = {"goal": "Say hello without tools."}
                receipt = request(opener, token, "/api/v1/tasks", goal)
                public_id = receipt["public_id"]
                path = "/api/v1/tasks/" + public_id
                deadline = time.monotonic() + 60
                row = request(opener, token, path)
                while row["state"] not in helper.TERMINAL_STATES and time.monotonic() < deadline:
                    time.sleep(.25)
                    row = request(opener, token, path)
                assert row["environment"] == settings.execution.leader_environment
                assert row["state"] == "SUCCEEDED"
                assert row["result"]["agent_authored"] is True
                assert row["result"]["status"] == "completed"
                assert row["result"]["summary"].strip() == helper.ANSWER
                first_task_seconds = time.monotonic() - started
                systemctl("stop")
                STAGE = "restart"
                restart_started = time.monotonic()
                systemctl("start")
                wait_ready(opener, token)
                assert request(opener, token, path)["result"] == row["result"]
                assert request(opener, token, "/api/v1/tasks", goal)["public_id"] == public_id
                restart_seconds = time.monotonic() - restart_started
            except Exception as error:
                error.smoke_stage = STAGE
                raise
            finally:
                STAGE = "restore"
                try:
                    systemctl("stop")
                finally:
                    restore_errors = []
                    for target, (content, info) in original.items():
                        try:
                            publish_file(target, content, info)
                        except OSError as error:
                            restore_errors.append(error)
                    if restore_errors:
                        raise RuntimeError("systemd smoke configuration restoration failed") from restore_errors[0]
            assert systemctl("show", "--property=ActiveState", "--value") == "inactive"
            for target, (content, info) in original.items():
                actual = target.stat()
                assert target.read_bytes() == content
                assert (actual.st_mode, actual.st_uid, actual.st_gid) == (info.st_mode, info.st_uid, info.st_gid)
            assert database_rows("SELECT COUNT(*) FROM tasks WHERE kind='goal'") == [[1]]
            assert database_rows("PRAGMA integrity_check") == [["ok"]]
            return {"passed": True, "state": row["state"], "environment": row["environment"],
                    "ready_seconds": ready_seconds, "first_task_seconds": first_task_seconds,
                    "restart_readback_seconds": restart_seconds, "same_key_unique_goal": True,
                    "original_configuration_restored": True, "service_state": "inactive",
                    "scope": "GitHub-hosted disposable VM, installed systemd service, real Pi, synthetic loopback model"}
        finally:
            model.shutdown()
            thread.join()


def main():
    try:
        result = run()
    except Exception as error:  # noqa: BLE001 -- never print credentials or full task responses on failure.
        print(json.dumps({"passed": False, "error_type": type(error).__name__,
                          "stage": getattr(error, "smoke_stage", STAGE)}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
