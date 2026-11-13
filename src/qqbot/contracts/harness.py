"""Harness selection and native-log operations stay behind the adapter boundary."""
from typing import Protocol

from qqbot.domain.tasks import HarnessCapabilities


class HarnessPort(Protocol):
    binding: dict
    capabilities: HarnessCapabilities
    projection_version: str

    def create(self, args, claim, client, process_id, session_id, capacity): ...
    def session_path(self, data_root, run, process): ...
    def export_session(self, path, **options): ...
    def mutation_calls(self, path, **options): ...
    def recovery_snapshot(self, path, **options): ...
    def review_snapshot(self, path, **options): ...
    def boundary_receipts(self, path): ...
    def unfinished_tools(self, path): ...
    def restore_tool_results(self, path, receipts): ...


def resolve_harness(core, binding):
    from qqbot.adapters.pi.adapter import PiAdapter

    adapters = getattr(core, "harnesses", {"pi": PiAdapter()})
    adapter = adapters.get(binding.get("harness", "pi"))
    if adapter is None or any(binding.get(key, value) != value for key, value in adapter.binding.items()):
        raise ValueError("persisted session requires an unavailable Harness/compression adapter")
    required = ("event_stream", "cancel", "session_persistence", "append_input", "interactive_questions")
    missing = [name for name in required if not getattr(adapter.capabilities, name)]
    if missing:
        raise ValueError("Harness missing required capabilities " + ", ".join(missing))
    return adapter
