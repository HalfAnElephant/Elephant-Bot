"""Offline wizard behavior with synthetic providers and disposable private files."""
import base64
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
    legacy_key = base64.b64encode(b"l" * 32).decode()
    credentials.write_text("QQBOT_MODEL_KEY=old-model-key\nQQBOT_ADMIN_TOKEN=old-admin-token\n"
                           f"QQBOT_CREDENTIAL_KEY={legacy_key}\n", encoding="utf8")
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

    def start(profile, *, pi_executable):
        assert pi_executable == "node_modules/.bin/pi"
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


def test_switch_updates_the_effective_model_when_console_saved_an_unrelated_field(tmp_path, monkeypatch, capsys):
    from elephant import config as runtime_config
    from elephant.configuration import ConfigurationStore

    directory = tmp_path / "private"
    keys = iter(("old-fixture-key", "new-fixture-key"))
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: next(keys))
    answers(monkeypatch, "", "", "", "", "")
    assert setup.run_setup(directory) == 0
    cfg = directory / "config.yaml"
    original = cfg.read_bytes()
    base = load_settings(cfg)
    other = base.model_copy(update={"send_budget_per_minute": base.send_budget_per_minute + 1})
    store = ConfigurationStore(base)
    # Production console save persists the entire effective Settings object.
    store.save(base, other)
    assert store.read().model == base.model

    answers(monkeypatch, "2", "2", "", "", "", "")
    assert setup.run_setup(directory) == 0
    effective = runtime_config.load_runtime_settings(load_settings(cfg))
    assert cfg.read_bytes() == original
    assert effective.model.model == "gpt-5.4-mini"
    assert effective.send_budget_per_minute == other.send_budget_per_minute
    assert effective.model.api_key_env != base.model.api_key_env
    values = dotenv_values(directory / "credentials.env", interpolate=False)
    assert values[effective.model.api_key_env] == "new-fixture-key"
    assert values[base.model.api_key_env] == "old-fixture-key"
    assert "模型：OpenAI" in capsys.readouterr().out


def test_wizard_does_not_overwrite_model_or_credentials_while_core_is_running(tmp_path, monkeypatch, capsys):
    from elephant.configuration import ConfigurationStore

    directory = tmp_path / "private"
    monkeypatch.setattr(setup.getpass, "getpass", lambda _prompt, **_opts: "synthetic-key")
    answers(monkeypatch, "", "", "", "", "")
    assert setup.run_setup(directory) == 0
    cfg = directory / "config.yaml"
    env = directory / "credentials.env"
    base = load_settings(cfg)
    store = ConfigurationStore(base)
    store.save(base, base)
    saved = [path.read_bytes() for path in (cfg, env, store.path)]

    with setup._exclusive_database(base.database):
        answers(monkeypatch, "2", "2", "", "")
        assert setup.run_setup(directory) == 1
    assert [path.read_bytes() for path in (cfg, env, store.path)] == saved
    assert "Elephant 正在运行" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("model_key", "admin_key"),
    [
        ("QQBOT_ADMIN_TOKEN", "ELEPHANT_ADMIN_TOKEN"),
        ("ELEPHANT_MODEL_KEY", "QQBOT_MODEL_KEY"),
        ("QQBOT_CREDENTIAL_KEY", "CUSTOM_ADMIN_TOKEN"),
        ("QQBOT_GITHUB_TOKEN", "CUSTOM_ADMIN_TOKEN"),
    ],
)
def test_wizard_refuses_reserved_credential_role_collisions_before_probe(
    model_key, admin_key, tmp_path, monkeypatch
):
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    cfg = directory / "config.yaml"
    cfg.write_text(json.dumps({
        "database": str(directory / "core.db"),
        "admin_token_env": admin_key,
        "model": {"endpoint": "https://example.invalid/v1/chat/completions", "model": "fixture",
                  "api_key_env": model_key},
    }))
    cfg.chmod(0o600)
    env = directory / "credentials.env"
    env.write_text("ELEPHANT_ADMIN_TOKEN=synthetic-admin-only\n")
    env.chmod(0o600)
    before = (cfg.read_bytes(), env.read_bytes())
    answers(monkeypatch, "", "1")
    monkeypatch.setattr(setup, "probe_model", lambda *_: pytest.fail("unsafe model probe must not be attempted"))
    assert setup.run_setup(directory) == 1
    assert (cfg.read_bytes(), env.read_bytes()) == before


def test_blank_credential_does_not_reuse_legacy_alias(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    env = directory / "credentials.env"
    env.write_text("ELEPHANT_MODEL_KEY=\nQQBOT_MODEL_KEY=stale-fixture-key\n")
    env.chmod(0o600)
    answers(monkeypatch, "", "", "", "", "")
    monkeypatch.setattr(setup.getpass, "getpass", lambda *_args, **_opts: "fresh-fixture-key")
    assert setup.run_setup(directory) == 0
    saved = dotenv_values(env, interpolate=False)
    selected = load_settings(directory / "config.yaml").model.api_key_env
    assert saved[selected] == "fresh-fixture-key"
    assert saved[setup.MODEL_KEY] == ""
    assert saved["QQBOT_MODEL_KEY"] == "stale-fixture-key"


def test_keep_endpoint_can_rotate_valid_key_without_model_probe(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    keys = iter(("old-working-fixture", "replacement-fixture"))
    monkeypatch.setattr(setup.getpass, "getpass", lambda *_args, **_opts: next(keys))
    answers(monkeypatch, "", "", "", "", "")
    assert setup.run_setup(directory) == 0
    before = load_settings(directory / "config.yaml").model

    monkeypatch.setattr(setup, "probe_model", lambda *_: pytest.fail("probe was explicitly skipped"))
    answers(monkeypatch, "3", "", "", "")
    assert setup.run_setup(directory) == 0
    after = load_settings(directory / "config.yaml").model
    assert after == before.model_copy(update={"api_key_env": after.api_key_env})
    assert after.api_key_env != before.api_key_env
    saved = dotenv_values(directory / "credentials.env", interpolate=False)
    assert saved[before.api_key_env] == "old-working-fixture"
    assert saved[after.api_key_env] == "replacement-fixture"


def test_dotenv_updates_operate_on_bindings_not_inside_multiline_values():
    unrelated = "OTHER='first\nELEPHANT_MODEL_KEY=phantom\nlast'\n"
    for raw in (unrelated, unrelated + "ELEPHANT_MODEL_KEY=''\n"):
        updated = setup._add_credentials(raw, {setup.MODEL_KEY: "synthetic-model-key"})
        assert updated.startswith(unrelated)
        assert setup._credentials(updated) == {
            "OTHER": "first\nELEPHANT_MODEL_KEY=phantom\nlast",
            setup.MODEL_KEY: "synthetic-model-key",
        }


def test_private_first_write_publishes_complete_file_without_replacing(tmp_path, monkeypatch):
    path = tmp_path / "config"
    real_link = setup.os.link

    def interrupt_before_publish(source, destination):
        assert source.read_text() == "complete payload"
        assert not destination.exists()
        raise KeyboardInterrupt

    monkeypatch.setattr(setup.os, "link", interrupt_before_publish)
    with pytest.raises(KeyboardInterrupt):
        setup._private_write(path, "complete payload")
    assert not list(tmp_path.iterdir())

    monkeypatch.setattr(setup.os, "link", real_link)
    setup._private_write(path, "already here")
    with pytest.raises(FileExistsError):
        setup._private_write(path, "unexpected replacement")
    assert path.read_text() == "already here"
    assert list(tmp_path.iterdir()) == [path]



def test_snapshot_durability_error_distinguishes_already_published(tmp_path, monkeypatch):
    import errno
    import os
    import stat

    from elephant.configuration import DurabilityError, atomic_write

    path = tmp_path / "runtime-config.json"
    path.write_text("prior")
    real_fsync = os.fsync

    def fail_directory_sync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(errno.EIO, "test directory sync failed")
        return real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_directory_sync)
    with pytest.raises(DurabilityError, match="may have been updated"):
        atomic_write(path, "published")
    assert path.read_text() == "published"
    assert list(tmp_path.iterdir()) == [path]



@pytest.mark.parametrize("vault", ["qq-credentials", "mail-credentials"])
@pytest.mark.parametrize("canonical", [None, ""])
def test_existing_vault_without_effective_key_blocks_before_probe(tmp_path, monkeypatch, capsys, vault, canonical):
    from elephant.adapters.qq_official.credential_store import load_credential, save_credential

    directory = tmp_path / "private"
    answers(monkeypatch, "", "", "", "", "")
    monkeypatch.setattr(setup.getpass, "getpass", lambda *_args, **_opts: "model-fixture")
    assert setup.run_setup(directory) == 0
    env = directory / "credentials.env"
    cfg = directory / "config.yaml"
    original_key = base64.b64encode(b"v" * 32).decode()
    prior_env = env.read_text()
    if canonical is None:
        # No canonical or legacy key at all.
        env.write_text("".join(line for line in prior_env.splitlines(keepends=True)
                               if not line.startswith(setup.CREDENTIAL_KEY + "=")))
    else:
        env.write_text(setup._add_credentials(prior_env, {setup.CREDENTIAL_KEY: canonical})
                       + f"QQBOT_CREDENTIAL_KEY={original_key}\n")
    with setup._setup_environment({setup.CREDENTIAL_KEY: original_key}, (setup.CREDENTIAL_KEY,)):
        reference = save_credential(directory / vault, "app-fixture", "sealed-fixture")
    before = (cfg.read_bytes(), env.read_bytes())
    monkeypatch.setattr(setup, "_load_wizard_model", lambda *_: pytest.fail("must reject prior to model prompt"))
    assert setup.run_setup(directory) == 1
    assert (cfg.read_bytes(), env.read_bytes()) == before
    assert "从备份恢复原密钥" in capsys.readouterr().out
    with setup._setup_environment({setup.CREDENTIAL_KEY: original_key}, (setup.CREDENTIAL_KEY,)):
        assert load_credential(directory / vault, reference, "app-fixture") == "sealed-fixture"


def test_vault_created_after_prompt_is_rechecked_under_database_lock(tmp_path, monkeypatch):
    directory = tmp_path / "private"
    original = setup._load_wizard_model

    def storage_appeared(*args, **kwargs):
        selection = original(*args, **kwargs)
        vault = directory / "mail-credentials"
        vault.mkdir()
        (vault / "existing.json").write_text("synthetic existing encrypted blob")
        return selection

    monkeypatch.setattr(setup, "_load_wizard_model", storage_appeared)
    monkeypatch.setattr(setup.getpass, "getpass", lambda *_args, **_opts: "fixture-key")
    answers(monkeypatch, "", "", "")
    assert setup.run_setup(directory) == 1
    assert not (directory / "config.yaml").exists()
    assert not (directory / "credentials.env").exists()


def test_wizard_fences_credential_only_edits_while_core_is_running(tmp_path, monkeypatch, capsys):
    directory = tmp_path / "private"
    monkeypatch.setattr(setup.getpass, "getpass", lambda *_args, **_opts: "new-fixture-key")
    answers(monkeypatch, "", "", "", "", "")
    assert setup.run_setup(directory) == 0
    cfg = directory / "config.yaml"
    env = directory / "credentials.env"
    before_settings = load_settings(cfg)
    raw = setup._add_credentials(env.read_text(), {before_settings.model.api_key_env: ""})
    env.write_text(raw)
    before = (cfg.read_bytes(), env.read_bytes())
    with setup._exclusive_database(before_settings.database):
        answers(monkeypatch, "", "")
        assert setup.run_setup(directory) == 1
    assert (cfg.read_bytes(), env.read_bytes()) == before
    assert "向导不会自动重启" in capsys.readouterr().out


def test_system_setup_checks_unit_and_fences_with_unprivileged_holder(tmp_path, monkeypatch):
    import sys
    from types import SimpleNamespace

    from elephant import setup_system

    monkeypatch.setattr(setup_system, "STATE", tmp_path)
    monkeypatch.setattr(setup_system, "ROOT", tmp_path)
    db = tmp_path / "core.db"
    settings = SimpleNamespace(database=str(db))
    cases = iter((False, True, True))
    monkeypatch.setattr(setup_system, "_service_is_stopped", lambda: next(cases))

    # Use a controlled child process and real file locking/pipe lifetime; never
    # invoke local runuser, systemctl, or an actual Elephant service.
    def child(command, database):
        assert command == "lock" and database == str(db)
        return [
            sys.executable, "-I", "-c", """
import fcntl, os, sys
fd = os.open(sys.argv[1] + '.core.lock', os.O_CREAT | os.O_RDWR, 0o600)
fcntl.flock(fd, fcntl.LOCK_EX)
sys.stdout.buffer.write(b'K')
sys.stdout.buffer.flush()
sys.stdin.buffer.read(1)
""", database,
        ]

    monkeypatch.setattr(setup_system, "_service_command", child)
    with pytest.raises(setup._DatabaseBusy), setup_system.system_configuration_lock(settings):
        pytest.fail("active system service must not be edited")
    assert not (tmp_path / "core.db.core.lock").exists()
    with (
        setup_system.system_configuration_lock(settings),
        pytest.raises(setup._DatabaseBusy),
        setup._exclusive_database(db),
    ):
        pytest.fail("the child must hold the same lock as Core")
    with setup._exclusive_database(db):
        pass


def test_pending_database_move_keeps_read_only_reopen_and_blocks_edit(tmp_path, monkeypatch, capsys):
    from elephant.configuration import ConfigurationStore

    directory = tmp_path / "private"
    monkeypatch.setattr(setup.getpass, "getpass", lambda *_args, **_opts: "fixture-key")
    answers(monkeypatch, "", "", "", "", "")
    assert setup.run_setup(directory) == 0
    cfg = directory / "config.yaml"
    env = directory / "credentials.env"
    base = load_settings(cfg)
    store = ConfigurationStore(base)
    store.save(base, base.model_copy(update={"database": str(directory / "pending-next.db")}))
    watched = (cfg, env, store.path)
    before = [path.read_bytes() for path in watched]

    # A read-only repeat is fine even when the existing daemon is on the old DB.
    with setup._exclusive_database(base.database):
        answers(monkeypatch, "", "", "", "")
        assert setup.run_setup(directory) == 0
        answers(monkeypatch, "3", "")
        assert setup.run_setup(directory) == 1
    assert [path.read_bytes() for path in watched] == before
    assert "管理页保存了不同的数据库位置" in capsys.readouterr().out


@pytest.mark.parametrize("start_mode", ("later", "foreground", "login"))
def test_profile_lock_covers_scan_and_start_handoff(tmp_path, monkeypatch, start_mode):
    directory = tmp_path / "private"
    monkeypatch.setattr(setup.getpass, "getpass", lambda *_args, **_opts: "synthetic-key")
    answers(monkeypatch, "", "", "", "", "")
    assert setup.run_setup(directory) == 0
    before = [(directory / name).read_bytes() for name in ("config.yaml", "credentials.env")]
    checked = []

    def competing_wizard(stage):
        # The second wizard must stop before consuming input or changing keys.
        assert setup.run_setup(directory) == 1
        assert [(directory / name).read_bytes() for name in ("config.yaml", "credentials.env")] == before
        checked.append(stage)

    def bind(ui, settings, values, *, system):
        competing_wizard("scan")
        return 0

    monkeypatch.setattr(setup, "_bind_qq", bind)
    original_choose = setup.SetupUI.choose

    def choose(ui, question, options, *, default=0):
        if question == "启动方式":
            competing_wizard("start-choice")
        return original_choose(ui, question, options, default=default)

    monkeypatch.setattr(setup.SetupUI, "choose", choose)
    if start_mode == "foreground":
        def start(settings, values, *, on_ready):
            competing_wizard("foreground")
            with setup._exclusive_database(settings.database):
                on_ready()
                # A running daemon permits a read-only wizard after handoff.
                with setup._configuration_lock(directory):
                    checked.append("released-after-ready")
                with pytest.raises(setup._DatabaseBusy), setup._exclusive_database(settings.database):
                    pytest.fail("the daemon must retain its database lock")

        monkeypatch.setattr(setup, "_start_local", start)
    elif start_mode == "login":
        from elephant import setup_macos

        monkeypatch.setattr(setup.platform, "system", lambda: "Darwin")

        def install(directory, **kwargs):
            competing_wizard("login")
            return True

        monkeypatch.setattr(setup_macos, "install_launch_agent", install)
    answers(monkeypatch, "", "", "1", {"later": "1", "foreground": "2", "login": "3"}[start_mode])
    assert setup.run_setup(directory) == 0
    assert checked[:2] == ["scan", "start-choice"]
    if start_mode == "foreground":
        assert checked[2:] == ["foreground", "released-after-ready"]
    elif start_mode == "login":
        assert checked[2:] == ["login"]
    with setup._configuration_lock(directory):
        pass
