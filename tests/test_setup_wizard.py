"""Offline wizard behavior with synthetic providers and disposable private files."""
import json
from io import StringIO
from pathlib import Path

import httpx
import pytest
from dotenv import dotenv_values

from elephant import setup
from elephant.config import ModelConfig, load_settings


def answers(monkeypatch, *responses):
    monkeypatch.setattr("sys.stdin", StringIO("".join(response + "\n" for response in responses)))


def test_first_setup_only_writes_private_local_files_and_resumes(tmp_path, monkeypatch, capsys):
    directory = tmp_path / "private"
    answers(monkeypatch, "", "", "", "", "")
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: "synthetic-model-key")
    assert setup.run_setup(directory) == 0
    config = directory / "config.yaml"
    env = directory / "credentials.env"
    before = (config.read_bytes(), env.read_bytes())
    for file in (config, env):
        assert file.stat().st_mode & 0o777 == 0o600
    assert directory.stat().st_mode & 0o777 == 0o700
    assert "synthetic-model-key" not in capsys.readouterr().out
    assert "synthetic-model-key" not in config.read_text()
    assert dotenv_values(env, interpolate=False)["ELEPHANT_MODEL_KEY"] == "synthetic-model-key"
    settings = load_settings(config)
    assert settings.permissions.default_effect == "deny"
    assert settings.execution.worker_environment == settings.execution.leader_environment == "sandbox"
    assert not Path(settings.database).exists()

    answers(monkeypatch, "", "", "", "")
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: pytest.fail("must not rotate model key"))
    assert setup.run_setup(directory) == 0
    assert before == (config.read_bytes(), env.read_bytes())


def test_known_partial_local_files_fill_missing_fields_without_losing_existing_settings(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    config = directory / "config.yaml"
    env = directory / "credentials.env"
    config.write_text('deployment_id: known\nmodel:\n  model: approved-model\n', encoding="utf8")
    config.chmod(0o600)
    env.write_text('# keep comment\nUNRELATED_KEY="literal-$value"\nELEPHANT_MODEL_KEY=""\n', encoding="utf8")
    env.chmod(0o600)
    answers(monkeypatch, "10", "https://example.test", "", "", "")
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: "replacement-fixture")
    assert setup.run_setup(directory) == 0
    assert load_settings(config).model.model == "approved-model"
    assert load_settings(config).deployment_id == "known"
    assert '# keep comment\nUNRELATED_KEY="literal-$value"\n' in env.read_text()
    assert dotenv_values(env, interpolate=False)["ELEPHANT_MODEL_KEY"] == "replacement-fixture"


def test_existing_symlink_is_not_written(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    other = tmp_path / "outside"
    other.write_text("keep", encoding="utf8")
    (directory / "credentials.env").symlink_to(other)
    answers(monkeypatch)
    assert setup.run_setup(directory) == 1
    assert other.read_text() == "keep"
    assert not (directory / "config.yaml").exists()


def test_interrupt_during_hidden_key_prompt_does_not_persist_credentials(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    answers(monkeypatch, "", "")

    def interrupted(_prompt, **_opts):
        raise KeyboardInterrupt

    monkeypatch.setattr(setup.getpass, "getpass", interrupted)
    assert setup.run_setup(directory) == 130
    assert not (directory / "config.yaml").exists()
    assert not (directory / "credentials.env").exists()


def test_failed_selected_model_probe_leaves_fresh_setup_retryable(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    answers(monkeypatch, "", "", "1", "")
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: "invalid-fixture")

    async def rejected(_model, _key):
        return False

    monkeypatch.setattr(setup, "probe_model", rejected)
    assert setup.run_setup(directory) == 1
    assert not (directory / "config.yaml").exists()
    assert not (directory / "credentials.env").exists()


async def test_model_probe_does_not_follow_redirect_or_forward_tools():
    calls = []

    def response(request):
        calls.append(request)
        assert request.url == "https://fixture.invalid/chat/completions"
        assert request.headers["authorization"] == "Bearer synthetic-key"
        payload = json.loads(request.content)
        assert payload["messages"] == [{"role": "user", "content": "Reply with OK."}]
        assert "tools" not in payload
        return httpx.Response(301, headers={"Location": "https://different.invalid/"})

    model = ModelConfig(endpoint="https://fixture.invalid/chat/completions", model="fixture")
    assert not await setup.probe_model(model, "synthetic-key", transport=httpx.MockTransport(response))
    assert len(calls) == 1


async def test_terminal_qq_uses_existing_session_contract_and_does_not_print_url(monkeypatch, capsys):
    import qrcode

    fake_link = "https://q.qq.com/qqbot/openclaw/connect.html?task_id=private-value"
    rendered = []

    class Code:
        def __init__(self, **_options):
            pass

        def add_data(self, value):
            rendered.append(value)

        def print_ascii(self, **_options):
            pass

    async def immediate(_seconds):
        pass

    monkeypatch.setattr(qrcode, "QRCode", Code)
    monkeypatch.setattr(setup.asyncio, "sleep", immediate)

    def response(request):
        assert request.headers["authorization"] == "Bearer synthetic-admin"
        if request.url.path.endswith("/status"):
            return httpx.Response(200, json={"adapters": [], "credential_key_ready": True,
                                             "automatic_available": True})
        if request.method == "POST":
            assert json.loads(request.content) == {"instance_id": "qq-official", "automatic": True}
            return httpx.Response(200, json={"session_id": "fixture", "state": "waiting", "connect_url": fake_link})
        assert request.method == "GET" and request.url.path.endswith("/sessions/fixture")
        return httpx.Response(200, json={"session_id": "fixture", "state": "connected"})

    async with httpx.AsyncClient(base_url=setup.LOOPBACK, transport=httpx.MockTransport(response),
                                headers={"Authorization": "Bearer synthetic-admin"}) as client:
        assert await setup.scan_qq(client) == 0
    assert rendered == [fake_link]
    assert "private-value" not in capsys.readouterr().out


async def test_unfinished_qr_cancels_session_without_touching_other_adapters(monkeypatch):
    requests = []

    async def immediate(_seconds):
        pass

    monkeypatch.setattr(setup.asyncio, "sleep", immediate)

    def response(request):
        requests.append((request.method, request.url.path))
        if request.url.path.endswith("/status"):
            return httpx.Response(200, json={"adapters": [], "credential_key_ready": True,
                                             "automatic_available": True})
        if request.method == "POST":
            return httpx.Response(200, json={"session_id": "fixture", "state": "creating"})
        return httpx.Response(200, json={"session_id": "fixture", "state": "expired"})

    async with httpx.AsyncClient(base_url=setup.LOOPBACK, transport=httpx.MockTransport(response)) as client:
        assert await setup.scan_qq(client) == 1
    assert requests == [
        ("GET", setup.ONBOARDING + "/status"),
        ("POST", setup.ONBOARDING + "/sessions"),
        ("GET", setup.ONBOARDING + "/sessions/fixture"),
        ("DELETE", setup.ONBOARDING + "/sessions/fixture"),
    ]


def test_existing_legacy_keys_are_carried_forward_without_rotation_on_same_service(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    config = directory / "config.yaml"
    config.write_text('model: {endpoint: "https://api.deepseek.com/chat/completions", model: "deepseek-flash"}\n',
                      encoding="utf8")
    config.chmod(0o600)
    credentials = directory / "credentials.env"
    credentials.write_text("QQBOT_MODEL_KEY=old-model-key\nQQBOT_ADMIN_TOKEN=old-admin-token\n"
                           "QQBOT_CREDENTIAL_KEY=old-credential-key\n", encoding="utf8")
    credentials.chmod(0o600)
    answers(monkeypatch, "", "", "", "")
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: pytest.fail("legacy key must be reused"))
    assert setup.run_setup(directory) == 0
    values = dotenv_values(credentials, interpolate=False)
    for suffix in ("MODEL_KEY", "ADMIN_TOKEN", "CREDENTIAL_KEY"):
        assert values["ELEPHANT_" + suffix] == values["QQBOT_" + suffix]


async def test_default_setup_allows_authenticated_first_task_but_not_host_execution(
        tmp_path, monkeypatch):
    from elephant.app import create_app
    from elephant.permissions import PermissionSubject

    directory = tmp_path / "private"
    answers(monkeypatch, "", "", "", "", "")
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: "synthetic-model-key")
    assert setup.run_setup(directory) == 0
    values = dotenv_values(directory / "credentials.env", interpolate=False)
    monkeypatch.setenv("ELEPHANT_ADMIN_TOKEN", values["ELEPHANT_ADMIN_TOKEN"])
    settings = load_settings(directory / "config.yaml")

    def reject(_request):
        pytest.fail("unexpected external request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(reject)) as provider:
        app = create_app(settings, run_background=False, model_client=provider)
        async with app.router.lifespan_context(app), httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=setup.LOOPBACK,
            headers={"Authorization": "Bearer " + values["ELEPHANT_ADMIN_TOKEN"]},
        ) as client:
            first = await client.post("/api/v1/tasks", json={"goal": "Say hello without tools"},
                                      headers={"Idempotency-Key": "offline-first-task"})
            assert first.status_code == 200, first.text
            assert first.json()["public_id"]
            readback = await client.get("/api/v1/tasks/" + first.json()["public_id"])
            assert readback.status_code == 200, readback.text
            assert (await client.get("/api/v1/permissions")).status_code == 200
        subject = PermissionSubject(adapter_instance_id="admin_api", identity_namespace="admin_api",
                                    user_id=settings.owner.id)
        for action in ("history.read", "notification.send"):
            assert app.state.core.permissions.check(subject, action, "conversation:internal-fixture").allowed
        assert not app.state.core.permissions.check(subject, "host.process.run", "workspace:*").allowed


def test_resuming_can_switch_service_without_reusing_key(tmp_path, monkeypatch, capsys):
    directory = tmp_path / "private"
    keys = iter(("old-fixture-key", "new-fixture-key"))
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: next(keys))
    answers(monkeypatch, "", "", "", "", "")
    assert setup.run_setup(directory) == 0

    checked = []

    async def verify(model, key):
        checked.append((model.endpoint, key))
        return True

    monkeypatch.setattr(setup, "probe_model", verify)
    answers(monkeypatch, "2", "2", "", "1", "", "")
    assert setup.run_setup(directory) == 0
    settings = load_settings(directory / "config.yaml")
    values = dotenv_values(directory / "credentials.env", interpolate=False)
    assert settings.model.api_key_env.startswith("ELEPHANT_MODEL_KEY_")
    assert values[settings.model.api_key_env] == "new-fixture-key"
    assert values["ELEPHANT_MODEL_KEY"] == "old-fixture-key"
    assert checked == [("https://api.openai.com/v1/chat/completions", "new-fixture-key")]
    output = capsys.readouterr().out
    for technical in ("old-fixture-key", "new-fixture-key", "api.openai.com", "config.yaml", "systemctl", "--env-file"):
        assert technical not in output
    assert "模型已响应" in output


def test_retries_with_new_key_and_no_candidate_persisted_before_success(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    keys = iter(("wrong-key", "right-key", "unused"))
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: next(keys))

    async def verify(_model, key):
        assert not (directory / "config.yaml").exists()
        assert not (directory / "credentials.env").exists()
        return key == "right-key"

    monkeypatch.setattr(setup, "probe_model", verify)
    answers(monkeypatch, "", "", "1", "3", "1", "", "")
    assert setup.run_setup(directory) == 0
    settings = load_settings(directory / "config.yaml")
    values = dotenv_values(directory / "credentials.env", interpolate=False)
    assert values[settings.model.api_key_env] == "right-key"
    assert "wrong-key" not in values.values()


def test_registered_service_is_not_default_for_unassigned_legacy_model_key(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    env = directory / "credentials.env"
    env.write_text("QQBOT_MODEL_KEY=unknown-origin-key\n")
    env.chmod(0o600)
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: "new-fixture-key")
    answers(monkeypatch, "2", "", "", "", "")
    assert setup.run_setup(directory) == 0
    settings = load_settings(directory / "config.yaml")
    values = dotenv_values(env, interpolate=False)
    assert values[settings.model.api_key_env] == "new-fixture-key"
    assert values["QQBOT_MODEL_KEY"] == "unknown-origin-key"


def test_foreground_start_is_opt_in_and_reuses_saved_config(tmp_path, monkeypatch, capsys):
    directory = tmp_path / "private"
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: "synthetic-key")
    starts = []

    def start(settings, values, *, on_ready):
        starts.append(settings.model.model)
        assert values["ELEPHANT_MODEL_KEY"] == "synthetic-key"
        assert (directory / "config.yaml").exists()
        on_ready()

    monkeypatch.setattr(setup, "_start_local", start)
    answers(monkeypatch, "", "", "", "", "")
    assert setup.run_setup(directory) == 0
    assert not starts
    answers(monkeypatch, "", "", "", "2")
    assert setup.run_setup(directory) == 0
    assert starts == ["deepseek-flash"]
    assert "Elephant 已运行" in capsys.readouterr().out


async def test_openai_probe_uses_completion_limit_and_no_tools():
    def response(request):
        payload = json.loads(request.content)
        assert "max_tokens" not in payload
        assert payload["max_completion_tokens"] == 512
        assert "tools" not in payload
        return httpx.Response(200, json={"choices": [{"message": {"content": "OK"}}]})

    model = ModelConfig(endpoint="https://api.openai.com/v1/chat/completions", model="gpt-5.4-mini")
    assert await setup.probe_model(model, "fixture-key", transport=httpx.MockTransport(response))


def test_can_return_from_model_picker_to_service_list(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: "synthetic-key")
    answers(monkeypatch, "1", "4", "2", "", "", "", "")
    assert setup.run_setup(directory) == 0
    assert load_settings(directory / "config.yaml").model.model == "gpt-5.4-mini"


def test_macos_login_start_is_explicit(tmp_path, monkeypatch, capsys):
    from elephant import setup_macos

    directory = tmp_path / "private"
    monkeypatch.setattr(setup.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: "synthetic-key")
    starts = []

    def start(profile):
        assert (profile / "config.yaml").exists()
        starts.append(profile)
        return True

    monkeypatch.setattr(setup_macos, "install_launch_agent", start)
    answers(monkeypatch, "", "", "", "", "")
    assert setup.run_setup(directory) == 0
    assert not starts
    answers(monkeypatch, "", "", "", "3")
    assert setup.run_setup(directory) == 0
    assert starts == [directory]
    assert "已向 macOS 提交启动请求" in capsys.readouterr().out
