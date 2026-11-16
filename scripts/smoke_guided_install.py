"""Run a first guided-install task through the installed CLI and the real Pi runner."""

import http.server
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from contextlib import contextmanager
from pathlib import Path

import yaml

from elephant.cli import SERVE_PORT
from elephant.config import Settings
from elephant.setup import _new_local_configuration

ANSWER = "Synthetic first task complete."
MODEL = "synthetic-fixture"
MODEL_KEY = "fixture-model-key"
ADMIN_TOKEN = secrets.token_urlsafe(32)
LOOPBACK = "127.0.0.1"
TERMINAL_STATES = {"SUCCEEDED", "FAILED", "NEEDS_RECONCILIATION", "BLOCKED", "CANCELLED"}


class ModelHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/chat/completions" or self.headers.get("Authorization") != f"Bearer {MODEL_KEY}":
            self.send_error(400, "Unexpected model request")
            return
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if body.get("model") != MODEL or not body.get("stream"):
            self.send_error(400, "Expected a streaming Pi request for the synthetic model")
            return

        chunk = {"id": "smoke-model-reply", "object": "chat.completion.chunk", "model": MODEL, "created": 1}
        start = {**chunk, "choices": [
            {"index": 0, "delta": {"role": "assistant", "content": ANSWER}, "finish_reason": None},
        ]}
        end = {**chunk, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
               "usage": {"prompt_tokens": 12, "completion_tokens": 5, "total_tokens": 17}}
        events = "".join(f"data: {json.dumps(part)}\n\n" for part in (start, end))
        data = (events + "data: [DONE]\n\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def request(opener, path, payload=None):
    headers = {"Authorization": f"Bearer {ADMIN_TOKEN}", "Content-Type": "application/json",
               "Idempotency-Key": "guided-smoke-first-task"}
    body = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(f"http://{LOOPBACK}:{SERVE_PORT}{path}", body, headers)
    with opener.open(req, timeout=2) as response:
        return json.load(response)


@contextmanager
def running_service(command, env, log_path, opener):
    # The production wrapper has a fixed port. Never submit a task to an existing service.
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((LOOPBACK, SERVE_PORT))
    with log_path.open("wb") as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=log, start_new_session=True)
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f"Elephant exited before readiness ({process.returncode})")
                try:
                    if request(opener, "/health/ready")["ready"] and process.poll() is None:
                        break
                except OSError:
                    pass
                time.sleep(0.2)
            else:
                raise RuntimeError("Timed out waiting for Elephant readiness")
            yield
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
                    raise RuntimeError("Elephant did not stop on SIGTERM")
                if process.returncode not in (0, -signal.SIGTERM):
                    raise RuntimeError(f"Elephant did not shut down normally ({process.returncode})")


def verify_result(row, public_id):
    assert row["public_id"] == public_id, row
    assert row["environment"] == "sandbox", row
    assert row["state"] == "SUCCEEDED", row
    assert row["result"]["agent_authored"] is True, row
    assert row["result"]["status"] == "completed", row
    assert row["result"]["summary"].strip() == ANSWER, row


def run_smoke(local, model_port):
    settings = Settings()
    config = _new_local_configuration(local)
    config["model"] = {"endpoint": f"http://{LOOPBACK}:{model_port}/chat/completions", "model": MODEL}
    cfg = local / "config.yaml"
    cfg.write_text(yaml.safe_dump(config), encoding="utf-8")
    env_file = local / "credentials.env"
    env_file.write_text(f"{settings.admin_token_env}={ADMIN_TOKEN}\n"
                        f"{settings.model.api_key_env}={MODEL_KEY}\n", encoding="utf-8")

    # Do not inherit provider credentials, proxies, or settings from the CI runner.
    env = {"PATH": os.environ["PATH"], "HOME": str(local), "NO_PROXY": f"{LOOPBACK},localhost",
           "LANG": "en_US.UTF-8", "PYTHONUTF8": "1", "TMPDIR": str(local)}
    command = ["./elephant", "serve", "--config", str(cfg), "--env-file", str(env_file)]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    payload = {"goal": "Say hello without tools."}
    with running_service(command, env, local / "serve.log", opener):
        public_id = request(opener, "/api/v1/tasks", payload)["public_id"]
        assert public_id
        path = f"/api/v1/tasks/{public_id}"
        deadline = time.monotonic() + 45
        row = request(opener, path)
        while row["state"] not in TERMINAL_STATES and time.monotonic() < deadline:
            time.sleep(0.25)
            row = request(opener, path)
        verify_result(row, public_id)

    with running_service(command, env, local / "restart.log", opener):
        verify_result(request(opener, path), public_id)
        assert request(opener, "/api/v1/tasks", payload)["public_id"] == public_id


def main():
    # A short directory keeps the Unix-domain socket within macOS and Linux limits.
    with tempfile.TemporaryDirectory(prefix="elephant-smoke-", dir="/tmp") as folder:
        local = Path(folder)
        with http.server.ThreadingHTTPServer((LOOPBACK, 0), ModelHandler) as model:
            thread = threading.Thread(target=model.serve_forever, daemon=True)
            thread.start()
            try:
                run_smoke(local, model.server_port)
            except Exception:
                for log in sorted(local.glob("*.log")):
                    print(f"--- {log.name} ---", file=sys.stderr)
                    print(log.read_text(encoding="utf-8", errors="replace")[-12000:], file=sys.stderr)
                raise
            finally:
                model.shutdown()
                thread.join()
    print("Guided install smoke passed (real Pi task and graceful restart readback)")


if __name__ == "__main__":
    main()
