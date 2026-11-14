"""Core-owned durable operation journal. Workers access this through authenticated Core APIs."""
import json
from dataclasses import asdict
from datetime import UTC, datetime

from sqlalchemy import or_, select

from qqbot.storage.action_schema import operations


class SQLiteActionLedger:
    def __init__(self, database):
        self.database = database

    async def prepare(self, operation_id, context, action, arguments_hash, arguments, *, scope_task_ids=None, deduplicate=False):
        with self.database.transaction() as connection:
            existing = connection.execute(select(operations).where(operations.c.id == operation_id)).mappings().first()
            if existing:
                if existing["task_id"] != context.task_id or existing["arguments_hash"] != arguments_hash:
                    raise ValueError("operation id conflicts with persisted target or arguments")
                return {"state": existing["state"], "operation_id": existing["id"], "result": json.loads(existing["result_json"])}
            unsafe_remote = (action.name.startswith(("github.", "icloud."))
                             and action.effect in {"write", "unknown"}) or action.name == "git.push"
            if unsafe_remote or deduplicate:
                query = select(operations).where(
                    operations.c.task_id.in_(scope_task_ids or [context.task_id]),
                    operations.c.arguments_hash == arguments_hash, operations.c.state != "FAILED")
                if deduplicate and not unsafe_remote:
                    query = query.where(or_(operations.c.run_id != context.run_id,
                        operations.c.state.in_(["PREPARED", "DISPATCHED", "UNKNOWN", "PARTIAL"])))
                previous = connection.execute(query.order_by(operations.c.updated_at.desc()).limit(1)).mappings().first()
                if previous:
                    return {"state": previous["state"], "operation_id": previous["id"],
                            "result": json.loads(previous["result_json"])}
            connection.execute(operations.insert().values(
                id=operation_id, task_id=context.task_id, run_id=context.run_id, fence=context.fence,
                arguments_hash=arguments_hash, action_json=json.dumps(asdict(action)),
                arguments_json=json.dumps(arguments, ensure_ascii=False), state="PREPARED", result_json="{}",
                updated_at=datetime.now(UTC).isoformat(),
            ))
            return {"state": "PREPARED", "result": {}}

    async def transition(self, operation_id, expected, state, result):
        allowed = {"PREPARED": {"DISPATCHED"}, "DISPATCHED": {"VERIFIED", "PARTIAL", "FAILED", "UNKNOWN"},
                   "UNKNOWN": {"VERIFIED", "PARTIAL", "FAILED"}}
        if state not in allowed.get(expected, set()):
            raise ValueError("invalid operation transition")
        with self.database.transaction() as connection:
            changed = connection.execute(operations.update().where(
                operations.c.id == operation_id, operations.c.state == expected
            ).values(state=state, result_json=json.dumps(result, ensure_ascii=False),
                     updated_at=datetime.now(UTC).isoformat()))
            return changed.rowcount == 1
