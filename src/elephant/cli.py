import argparse
import fcntl
import json
import os
import shutil
import socket
import sqlite3
import stat
import sys
import tempfile
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from dotenv import load_dotenv

from elephant.config import Settings, load_runtime_settings, load_settings
from elephant.credentials_env import credential_env

SERVE_HOST = "127.0.0.1"
SERVE_PORT = 8080


def probe_local_database():
    with tempfile.TemporaryDirectory() as folder:
        db = str(Path(folder) / "doctor.db")
        migrate(db)
        with sqlite3.connect(db) as connection:
            if connection.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                raise RuntimeError("SQLite integrity check failed")
            connection.execute("CREATE VIRTUAL TABLE probe USING fts5(content)")


def executable_paths(settings):
    return {"node": shutil.which("node"), "pi": shutil.which(settings.orchestration.pi_executable),
            "ffmpeg": shutil.which("ffmpeg")}


def share_safe_doctor(settings: Settings, sqlite_status: str) -> dict:
    """Build only allowlisted statuses; never serialize config or probe remote services."""
    from elephant.adapters.qq_official.credential_store import master_key

    model = settings.model
    model_state = ("not_configured" if not (model.endpoint or model.model) else
                   "configured" if model.endpoint and model.model and credential_env(model.api_key_env) else "incomplete")
    adapters = [adapter for adapter in settings.adapters if adapter.enabled]
    qq_state = ("not_configured" if not adapters else
                "configured" if all(adapter.credential_ref or credential_env(adapter.secret_env)
                                    for adapter in adapters) else "incomplete")
    try:
        master_key()
    except ValueError:
        onboarding = "credential_missing_or_invalid"
    else:
        onboarding = "credential_available"

    memory = settings.memory.openviking
    binding = next((binding for binding in memory.bindings
                    if (binding.owner_id, binding.workspace_id) == (settings.owner.id, "personal")), None)
    if not memory.endpoint:
        memory_state = "not_configured"
    elif not memory.private_enabled:
        memory_state = "disabled"
    else:
        memory_state = "configured" if binding and credential_env(binding.api_key_env) else "incomplete"

    return {
        "checks": {
            "configuration": "passed",
            "sqlite": sqlite_status,
            "dependencies": {name: "available" if path else "missing" for name, path in executable_paths(settings).items()},
            "model": {"configuration": model_state, "connectivity": "not_tested"},
            "qq": {"configuration": qq_state, "connectivity": "not_tested", "scan_onboarding": onboarding},
            "openviking": {"configuration": memory_state, "connectivity": "not_tested"},
            "worker_isolation": {"verification": "not_tested"},
        },
        "effective": {
            "execution": {"leader": settings.execution.leader_environment, "worker": settings.execution.worker_environment},
            "permissions": {"preset": settings.permissions.preset, "default_effect": settings.permissions.default_effect},
            "listener": {"host": SERVE_HOST, "port": SERVE_PORT, "scope": "builtin_serve_only"},
        },
    }


def deprecated_quota_warnings(settings: Settings) -> list[str]:
    fields = {
        "execution": ("max_active", "sustained_limit"),
        "orchestration": ("pending_inputs_per_owner", "pending_root_tasks_per_owner",
                          "child_parallelism", "max_task_depth", "max_nodes_per_root"),
    }
    return [
        f"{section}.{field} is deprecated and no longer enforces a quota; remove it from the startup YAML. "
        "Use orchestration.model_request_limit for model concurrency and execution budgets for task limits."
        for section, names in fields.items()
        for field in names if field in getattr(settings, section).model_fields_set
    ]


def configuration_warnings(settings):
    warnings = []
    if not settings.model.endpoint or not settings.model.model:
        warnings.append("Set model.endpoint and model.model before submitting tasks or chatting.")
    if not credential_env(settings.admin_token_env):
        warnings.append("Administrator credential is empty. Check admin_token_env and load the private environment file.")
    if not credential_env(settings.model.api_key_env):
        warnings.append("Model credential is empty. Check model.api_key_env and load the private environment file.")
    from elephant.adapters.qq_official.credential_store import master_key
    try:
        master_key()
    except ValueError:
        warnings.append("QQ scan onboarding requires ELEPHANT_CREDENTIAL_KEY, a Base64-encoded 32-byte key.")
    memory = settings.memory.openviking
    if memory.endpoint:
        if not memory.bindings:
            warnings.append("OpenViking has an endpoint but no bindings; memory is disabled. See docs/openviking.md.")
        elif not any(binding.owner_id == settings.owner.id for binding in memory.bindings):
            warnings.append("OpenViking has no binding for owner.id; this owner's memory is disabled.")
        if any(not credential_env(binding.api_key_env) for binding in memory.bindings):
            warnings.append("OpenViking USER credential is empty. Check memory.openviking.bindings[].api_key_env.")
    return warnings


def migrate(path: str, *, backup=True):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    cfg = Config("alembic.ini")
    cfg.set_main_option("sqlalchemy.url", f"sqlite:///{path}")
    target_revision = ScriptDirectory.from_config(cfg).get_current_head()
    if backup and Path(path).exists() and Path(path).stat().st_size:
        from datetime import UTC, datetime
        with sqlite3.connect(path) as source:
            try:
                revision = source.execute("SELECT version_num FROM alembic_version").fetchone()
            except sqlite3.OperationalError:
                revision = None
            if not revision or revision[0] != target_revision:
                backup_path = str(path) + f".before-{target_revision}-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f") + ".db"
                descriptor = os.open(backup_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                os.close(descriptor)
                with sqlite3.connect(backup_path) as destination:
                    source.backup(destination)
    from elephant.storage.legacy_upgrade import bridge_experimental_0010
    bridge_experimental_0010(path)
    command.upgrade(cfg, "head")


def main():
    parser = argparse.ArgumentParser(prog="elephant")
    parser.add_argument(
        "command",
        choices=["setup", "doctor", "validate", "migrate", "serve", "schema", "backup", "restore", "export-deletions"],
    )
    parser.add_argument("--config", default="configs/example.yaml")
    parser.add_argument("--env-file", type=Path, help="read an explicit dotenv file without running shell code")
    parser.add_argument("--setup-dir", type=Path, help="directory for setup configuration and credentials")
    parser.add_argument("--system", action="store_true", help="setup for the installed elephant.service")
    parser.add_argument("--destination")
    parser.add_argument("--snapshot")
    parser.add_argument("--deletion-ledger")
    parser.add_argument("--share-safe", action="store_true", help="doctor only: print redacted offline checks")
    args = parser.parse_args()
    if args.share_safe and args.command != "doctor":
        parser.error("--share-safe requires doctor")
    if args.command == "setup":
        if args.env_file or args.config != "configs/example.yaml":
            parser.error("setup uses --setup-dir, not --config or --env-file")
        if args.system and args.setup_dir is not None:
            parser.error("--system uses /etc/elephant")
        from elephant.setup import run_setup

        directory = args.setup_dir or (Path("/etc/elephant") if args.system else Path.home() / ".config" / "elephant")
        return run_setup(directory, system=args.system)
    if args.system or args.setup_dir is not None:
        parser.error("--system and --setup-dir require setup")
    try:
        if args.env_file is not None:
            if not args.env_file.is_file():
                raise ValueError("environment file unavailable")
            load_dotenv(args.env_file, override=False, interpolate=False)
        startup_settings = load_settings(args.config)
        settings = load_runtime_settings(startup_settings)
    except Exception:
        if not args.share_safe:
            raise
        print(json.dumps({"checks": {"configuration": "failed", "sqlite": "not_tested"}}))
        return 2
    # Snapshots materialize defaults; warn only for explicit startup YAML.
    legacy_warnings = deprecated_quota_warnings(startup_settings)
    if args.command in {"backup", "restore", "export-deletions"}:
        from elephant.storage.operations import backup, export_deletions, restore

        if not args.destination:
            parser.error("--destination required")
        if args.command == "backup":
            backup(settings.database, args.destination)
        elif args.command == "export-deletions":
            export_deletions(settings.database, args.destination)
        else:
            if not args.snapshot or not args.deletion_ledger:
                parser.error("restore requires --snapshot and a current --deletion-ledger")
            restore(args.snapshot, args.destination, args.deletion_ledger)
        print(args.command + " complete")
    elif args.command == "schema":
        print(json.dumps(Settings.model_json_schema(), ensure_ascii=False, indent=2))
    elif args.command == "validate":
        print("configuration valid")
        for warning in configuration_warnings(settings) + legacy_warnings:
            print("warning: " + warning, file=sys.stderr)
    elif args.command == "migrate":
        migrate(settings.database)
        print("migration complete")
    elif args.command == "doctor":
        try:
            probe_local_database()
        except Exception:
            if not args.share_safe:
                raise
            print(json.dumps(share_safe_doctor(settings, "failed")))
            return 1
        if args.share_safe:
            print(json.dumps(share_safe_doctor(settings, "passed")))
            return 0
        dependencies = executable_paths(settings)
        warnings = configuration_warnings(settings) + legacy_warnings
        warnings.extend(f"Executable {name} was not found; install it or correct its configured path."
                        for name, path in dependencies.items() if path is None)
        print(
            json.dumps(
                {
                    "offline": "passed",
                    "adapters_configured": len(settings.adapters),
                    "qq_live": "not_tested",
                    "pi_model_run": "not_tested",
                    "dependencies": dependencies,
                    "warnings": warnings,
                }
            )
        )
    else:
        for warning in legacy_warnings:
            print("warning: " + warning, file=sys.stderr)
        serve_local(startup_settings, settings)


def serve_local(startup_settings, settings, *, on_ready=None):
    """Shared server launch for the direct CLI and the interactive setup."""
    import uvicorn

    from elephant.app import create_app

    Path(settings.database).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = os.open(settings.database + ".core.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if settings.tasks_enabled or settings.interactive_execution_enabled:
            clean_stale_socket(settings.internal_socket)
        app = create_app(startup_settings)
        if on_ready is None:
            uvicorn.run(app, host=SERVE_HOST, port=SERVE_PORT)
        else:
            class GuidedServer(uvicorn.Server):
                async def startup(self, sockets=None):
                    await super().startup(sockets=sockets)
                    if self.started:
                        on_ready()

            server = GuidedServer(uvicorn.Config(
                app, host=SERVE_HOST, port=SERVE_PORT, log_level="critical", access_log=False,
            ))
            server.run()
            if not server.started:
                raise RuntimeError("Elephant did not start")
    finally:
        os.close(lock)


def clean_stale_socket(path):
    """Called only while the database's exclusive Core process lock is held."""
    path = Path(path)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError("internal socket path has unexpected ownership or type")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(1)
        try:
            probe.connect(str(path))
        except ConnectionRefusedError:
            path.unlink()
        else:
            raise RuntimeError("another internal listener is still active")
