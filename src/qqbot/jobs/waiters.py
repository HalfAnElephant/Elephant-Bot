"""Read-only external observations wake durable tasks without model polling."""
import asyncio
import hashlib
import inspect
import json
import time

import httpx

from qqbot.capabilities.types import ToolContext


class ExternalWaiters:
    def __init__(self, call, registry):
        self.call, self.registry = call, registry
        self.next_poll = {}

    async def once(self):
        records = await self.call("waiters", {})
        live = {record["id"] for record in records}
        self.next_poll = {key: value for key, value in self.next_poll.items() if key in live}
        for record in records:
            if self.next_poll.get(record["id"], 0) > time.monotonic():
                continue
            condition = record["condition"]
            self.next_poll[record["id"]] = time.monotonic() + max(15, min(3600, condition.get("interval_seconds", 30)))
            context = ToolContext(task_id=record["task_id"], run_id="waiter:" + record["id"], fence=record["revision"],
                principal=record["principal"], workspace="", repository=record.get("repository"), host=record.get("host", "github.com"))
            try:
                if condition["capability"] == "job.inspect":
                    value = await self.call("wait-job-inspect", {"wait_id": record["id"], "revision": record["revision"]})
                else:
                    capability, execute, classify = self.registry.entries[condition["capability"]]
                    action = classify(condition["arguments"], context)
                    if action.effect != "read" or not action.name.startswith(("github.", "web.")):
                        raise PermissionError("external waiter only observes registered remote read capabilities")
                    await self.call("wait-authorize", {"wait_id": record["id"], "revision": record["revision"],
                        "action": action.name, "resources": action.resources})
                    value = execute(condition["arguments"], context)
                    value = await asyncio.wait_for(value, capability.timeout_seconds) if inspect.isawaitable(value) else value
                if isinstance(value, dict) and "stdout" in value:
                    if value.get("exit_code") or value.get("truncated"):
                        continue
                    value = json.loads(value["stdout"])
                observed = value
                path = condition["path"]
                if condition["capability"] == "job.inspect":
                    path = path.removeprefix("data.")
                for part in path.split("."):
                    if part:
                        observed = observed[int(part)] if isinstance(observed, list) else observed[part]
                if observed != condition["equals"]:
                    continue
                encoded = json.dumps(value, ensure_ascii=False, sort_keys=True)
                if len(encoded.encode()) > 60000:
                    encoded = json.dumps({"observed": observed, "path": condition["path"], "truncated": True})
                await self.call("wait-observed", {"wait_id": record["id"], "revision": record["revision"],
                    "observation": encoded, "source_id": "external:" + hashlib.sha256((record["id"] + encoded).encode()).hexdigest()})
            except (ValueError, KeyError, IndexError, TypeError, RuntimeError, PermissionError, OSError, TimeoutError, httpx.HTTPError):
                # A failed observation is not a satisfied condition or a reason to rerun business actions.
                continue

    async def run(self):
        while True:
            try:
                await self.once()
            except (httpx.HTTPError, OSError):
                pass
            await asyncio.sleep(1)
