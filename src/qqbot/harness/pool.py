"""Bounded process ownership; only committed quiescent runs return to the idle pool."""
import asyncio
import logging
import time
from uuid import uuid4

logger = logging.getLogger(__name__)


class PoolAcquisitionError(RuntimeError):
    def __init__(self, runner):
        super().__init__("runner acquisition failed and stopping was not verified")
        self.runner = runner


class SessionPool:
    def __init__(self, factory, *, max_resident=4, max_idle=2, ttl=300):
        if not 0 <= max_idle <= max_resident or max_resident < 1:
            raise ValueError("invalid resident/idle limits")
        self.factory, self.max_resident, self.max_idle, self.ttl = factory, max_resident, max_idle, ttl
        self.entries = {}
        self.lock = asyncio.Lock()

    async def _evict(self, entry):
        try:
            stopped = await entry["runner"].stop()
        except Exception:
            entry["state"] = "UNKNOWN"
            logger.exception("Runner stop failed process_id=%s", entry["id"])
            return False
        except BaseException:
            entry["state"] = "UNKNOWN"
            raise
        if stopped:
            self.entries.pop(entry["id"], None)
        else:
            entry["state"] = "UNKNOWN"
        return stopped

    async def acquire(self, claim):
        key = claim.get("session_id") or claim.get("context", {}).get("key") or "task:" + claim["task_id"]
        if not claim.get("session_id") and (claim.get("plan", {}).get("workspace_ref") or claim.get("delivery") == "DETACHED"):
            key = "task:" + claim["task_id"]
        async with self.lock:
            limits = claim.get("context", {}).get("pool")
            if limits:
                if not 0 <= limits["max_idle"] <= limits["max_resident"] <= 32 or limits["max_resident"] < 1 or limits["ttl"] < 1:
                    raise ValueError("invalid host pool limits")
                self.max_resident, self.max_idle, self.ttl = limits["max_resident"], limits["max_idle"], limits["ttl"]
            await self._sweep()
            for entry in list(self.entries.values()):
                if entry["key"] == key and entry["state"] == "IDLE":
                    entry["state"] = "BUSY"
                    try:
                        await entry["runner"].bind(claim)
                    except Exception:  # noqa: BLE001 -- A verified stop permits a cold runner.
                        if not await self._evict(entry):
                            raise PoolAcquisitionError(entry["runner"]) from None
                        continue
                    except BaseException:
                        await self._evict(entry)
                        raise
                    return entry["runner"]
            if len(self.entries) >= self.max_resident:
                idle = sorted((e for e in self.entries.values() if e["state"] == "IDLE"), key=lambda e: e["used"])
                if not idle or not await self._evict(idle[0]):
                    raise RuntimeError("resident process capacity exhausted; active and unknown processes retained")
            process_id, session_id = str(uuid4()), claim.get("session_id") or str(uuid4())
            runner = self.factory(claim, process_id, session_id)
            entry = {"id": process_id, "key": key, "state": "BUSY", "used": time.monotonic(), "runner": runner}
            self.entries[process_id] = entry
            runner.pool_id = process_id
            try:
                await runner.bind(claim)
            except Exception:
                if not await self._evict(entry):
                    raise PoolAcquisitionError(runner) from None
                raise
            except BaseException:
                await self._evict(entry)
                raise
            return runner

    async def release(self, runner, committed):
        async with self.lock:
            entry = self.entries.get(runner.pool_id)
            if not entry:
                return
            if committed and runner.quiescent_verified and not runner.stopped_verified:
                entry.update(state="IDLE", used=time.monotonic())
            else:
                await self._evict(entry)
            await self._sweep()

    async def _sweep(self):
        idle = sorted((e for e in self.entries.values() if e["state"] == "IDLE"), key=lambda e: e["used"])
        for index, entry in enumerate(idle):
            if time.monotonic() - entry["used"] >= self.ttl or len(idle) - index > self.max_idle:
                await self._evict(entry)

    async def sweep(self):
        async with self.lock:
            await self._sweep()

    async def retire_sessions(self, session_ids):
        """Stop obsolete idle sessions while their busy runs finish the stop protocol."""
        async with self.lock:
            for entry in list(self.entries.values()):
                if entry["key"] in session_ids and entry["state"] != "BUSY":
                    await self._evict(entry)
            return not any(entry["key"] in session_ids for entry in self.entries.values())

    async def close(self):
        async with self.lock:
            for entry in list(self.entries.values()):
                await self._evict(entry)
