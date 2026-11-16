"""Fault-injection coverage for the host task's SQLite transaction boundary."""

import pytest
from sqlalchemy import func, select

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.storage.agent_schema import dialogues
from qqbot.storage.schema import inbox
from qqbot.storage.task_schema import tasks


class InterruptedBeforeCommit(RuntimeError):
    """The in-process test interrupts a completed write before SQLite commit."""


def row_counts(core):
    with core.db.engine.connect() as conn:
        return tuple(conn.scalar(select(func.count()).select_from(table)) for table in (inbox, dialogues, tasks))


async def test_host_task_precommit_interruption_leaves_no_partial_receipt(tmp_path, clock, monkeypatch):
    settings = Settings(database=str(tmp_path / 'precommit.db'))
    core = create_app(settings, clock=clock, run_background=False).state.core
    key = 'retry-precommit'
    submit = core.tasks.graph.submit

    def interrupt_after_writes(conn, *args, **kwargs):
        submit(conn, *args, **kwargs)
        assert conn.scalar(select(func.count()).select_from(inbox)) == 1
        assert conn.scalar(select(func.count()).select_from(tasks)) == 2
        raise InterruptedBeforeCommit

    try:
        with monkeypatch.context() as patch:
            patch.setattr(core.tasks.graph, 'submit', interrupt_after_writes)
            with pytest.raises(InterruptedBeforeCommit):
                core.enqueue_admin_task('整理本地资料', idempotency_key=key)
    finally:
        await core.stop()
        core.db.close()

    reopened = create_app(settings, clock=clock, run_background=False).state.core
    try:
        assert row_counts(reopened) == (0, 0, 0)
        receipt = reopened.enqueue_admin_task('整理本地资料', idempotency_key=key)
        repeated = reopened.enqueue_admin_task('整理本地资料', idempotency_key=key)
        assert receipt['task_id'] == repeated['task_id']
        assert row_counts(reopened) == (1, 1, 2)
    finally:
        await reopened.stop()
        reopened.db.close()
