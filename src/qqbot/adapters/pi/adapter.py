"""Pi implementation of the host Harness and native-log contracts."""
import json
from pathlib import Path
from typing import ClassVar

from qqbot.harness.pi import PiHarness

from . import sessions


class PiAdapter:
    binding: ClassVar[dict] = {"harness": "pi", "contract_version": "elephant-1", "package_version": "1.5.0",
               "compression": "native"}
    capabilities = PiHarness.capabilities
    projection_version = "pi-original-v2"
    export_session = staticmethod(sessions.export_session)
    mutation_calls = staticmethod(sessions.mutation_calls)
    recovery_snapshot = staticmethod(sessions.recovery_snapshot)
    review_snapshot = staticmethod(sessions.review_snapshot)
    boundary_receipts = staticmethod(sessions.boundary_receipts)
    unfinished_tools = staticmethod(sessions.unfinished_tools)

    def __init__(self, executable=None):
        self.executable = executable or "node_modules/.bin/pi"

    def restore_tool_results(self, path, receipts):
        return sessions.restore_tool_results(path, receipts, pi_executable=self.executable)

    def create(self, args, claim, client, process_id, session_id, capacity):
        from qqbot.jobs.worker import GeneralRun

        return GeneralRun(args, claim, client, process_id, session_id, capacity)

    def session_path(self, data_root, run, process):
        identity = json.loads(process["identity_json"]) if process else {}
        path = Path(identity.get("actual_session_file") or run["session"])
        return path if path.is_absolute() else Path(data_root) / "pi" / path
