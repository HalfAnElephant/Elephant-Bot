"""QQ presentation and persistent pacing, without platform calls or real sleeps."""
import asyncio
import json
from datetime import datetime, timedelta

import pytest
from sqlalchemy import select, update

from qqbot.domain import ConversationRef, DeliveryCheck, OutboundEnvelope, SendReceipt, Surface
from qqbot.outbox.presentation import plain_text, split_text
from qqbot.outbox.service import Outbox
from qqbot.storage.schema import outbox


@pytest.fixture
def target():
    return ConversationRef(deployment_id="test", adapter_instance_id="qq", bot_identity_id="bot",
                           surface=Surface.PRIVATE, remote_conversation_id="a")


class Adapter:
    def __init__(self, clock, receipts=None):
        self.clock, self.sent = clock, []
        self.receipts = list(receipts or [])
        self.started, self.release = asyncio.Event(), None

    async def validate_delivery(self, envelope):
        return DeliveryCheck(allowed=True, reason="OK")

    async def send(self, envelope):
        self.sent.append((envelope, self.clock.now()))
        self.started.set()
        if self.release and envelope.target.remote_conversation_id == "a":
            await self.release.wait()
        return self.receipts.pop(0) if self.receipts else SendReceipt(state="SENT", platform_message_id=str(len(self.sent)))


def enqueue(service, db, target, text, origin="message"):
    with db.transaction() as connection:
        return service.enqueue_text(connection, target=target, text=text, origin_kind="chat", origin_id=origin)


def rows(db):
    with db.engine.connect() as connection:
        return connection.execute(select(outbox).order_by(outbox.c.created_at, outbox.c.part_no)).mappings().all()


@pytest.mark.parametrize(("source", "expected"), [
    ("# 标题\n这是**重点**和_强调_。", "标题\n这是重点和强调。"),
    ("[**标题**](https://example.org/a_(b))", "标题 (https://example.org/a_(b))"),
    ("1. 标题\n2) 下一项", "1）标题\n2）下一项"),
    ("标题\n===\n下一段", "标题\n下一段"),
    ("/研究 <目标> /任务 <编号> /人格 <JSON覆盖>", "/研究 <目标> /任务 <编号> /人格 <JSON覆盖>"),
    ("`a * b + foo_bar` 和 https://example.org/_literal_/path", "a * b + foo_bar 和 https://example.org/_literal_/path"),
    (r"文件 /tmp/_data_/a.py 和 C:\_data_\file.py", r"文件 /tmp/_data_/a.py 和 C:\_data_\file.py"),
    ("![图示](https://example.org/图.png) 与 <https://example.org/x>", "图示 (https://example.org/图.png) 与 https://example.org/x"),
    ("[来源][source]\n\n[source]: https://example.org/page", "来源 (https://example.org/page)\n\n"),
    ("- 项目\n> 引用", "• 项目\n引用"),
    ("[source]: https://example.org/unused", "source (https://example.org/unused)"),
    ("见 [source]\n[source]: https://example.org/x", "见 source (https://example.org/x)\n"),
    (r"[来源](https://example.org/a\(b\))", "来源 (https://example.org/a(b))"),
])
def test_plain_text_common_markdown_and_literal_information(source, expected):
    assert plain_text(source) == expected


def test_code_fences_preserve_code_body_and_operators():
    body = '#include <iostream>\na_b = x * y\npath = "_literal_"\nprint("<目标>", "**text**")\n'
    assert plain_text("```python\n" + body + "```\n") == body
    assert plain_text("~~~\n" + body + "~~~") == body
    assert plain_text("    a * b + _literal_\n") == "    a * b + _literal_\n"


def test_table_pipes_removed_without_losing_columns_or_literal_code():
    source = "| 名称 | 内容 |\n| :--- | ---: |\n| 链接 | [来源](https://example.org/x) |\n| 运算 | `a | b` |\n"
    assert plain_text(source) == "名称　内容\n链接　来源 (https://example.org/x)\n运算　a | b\n"


@pytest.mark.parametrize("source", ["", "  \n", "---", "***", "```\n```", "~~~\n~~~"])
def test_empty_formatting_output_rejected(source, db, clock, target):
    service = Outbox(db, clock)
    with pytest.raises(ValueError, match="empty"):
        enqueue(service, db, target, source)
    assert rows(db) == []


def test_semantic_split_is_lossless_and_prefers_paragraphs_then_sentences():
    text = "第一段。\n\n" + "这是第二段的一句话。" * 8
    parts = split_text(text, limit=25)
    assert parts[0] == "第一段。\n\n"
    assert all(len(part) <= 25 for part in parts)
    assert "".join(parts) == text
    assert all(part.endswith(("。", "\n")) for part in parts)


def test_split_keeps_links_whole_when_they_fit_one_message():
    url = "https://example.org/long_path?value=123456"
    text = "说明 " + "前" * 30 + " " + url + " 后续文字。" * 8
    parts = split_text(text, limit=55)
    assert "".join(parts) == text and all(len(part) <= 55 for part in parts)
    assert sum(url in part for part in parts) == 1


def test_unavoidable_long_atom_splits_without_dropping_characters():
    text = "长" * 100 + "https://example.org/" + "x" * 100
    parts = split_text(text, limit=30)
    assert "".join(parts) == text
    assert all(0 < len(part) <= 30 for part in parts)


async def test_both_enqueue_paths_normalize_once_and_preserve_code(db, clock, target):
    service = Outbox(db, clock)
    other = target.model_copy(update={"remote_conversation_id": "b"})
    code = 'x * y\nname = "**literal**"\n'
    with db.transaction() as connection:
        identity = service.enqueue(connection, OutboundEnvelope(delivery_id="direct", target=target,
            text="这是**重点**", origin_id="direct", origin_kind="command", expires_at=clock.now() + timedelta(minutes=5)))
    assert identity == "direct"
    enqueue(service, db, other, "```python\n" + code + "```", "code")
    adapter = Adapter(clock)
    await service.dispatch_once({"qq": adapter})
    assert {item.text for item, _ in adapter.sent} == {"这是重点", code}
    assert all(json.loads(row["envelope_json"])["plain_text"] for row in rows(db))
    assert all(set(json.loads(row["receipt_json"])) == {"state", "platform_message_id", "error_code"} for row in rows(db))


async def test_legacy_pending_and_retry_keep_code_literal(db, clock, target):
    service = Outbox(db, clock)
    envelope = OutboundEnvelope(delivery_id="legacy", target=target,
        text='```python\nname = "**literal**"\nx * y\n```', origin_id="legacy", expires_at=clock.now() + timedelta(minutes=5))
    with db.transaction() as connection:
        service._enqueue_one(connection, envelope)
    adapter = Adapter(clock, [SendReceipt(state="RETRYABLE", error_code="NETWORK"), SendReceipt(state="SENT")])
    await service.dispatch_once({"qq": adapter})
    assert rows(db)[0]["state"] == "RETRYABLE"
    clock.advance(10)
    await Outbox(db, clock).dispatch_once({"qq": adapter})
    assert [item.text for item, _ in adapter.sent] == ['name = "**literal**"\nx * y\n'] * 2
    assert rows(db)[0]["state"] == "SENT"


async def test_parts_remain_ordered_and_jitter_survives_restart(db, clock, target, monkeypatch):
    import qqbot.outbox.service as module
    identities = iter(("zz-last-lexically", "aa-first-lexically", "mm-middle"))
    monkeypatch.setattr(module, "uuid4", lambda: next(identities))
    service = Outbox(db, clock)
    text = "甲" * 1200 + "\n\n" + "乙" * 1200 + "\n\n" + "丙" * 1200
    ids = enqueue(service, db, target, text)
    adapter = Adapter(clock)
    await service.dispatch_once({"qq": adapter}, budget=0)
    assert [item.part_no for item, _ in adapter.sent] == [0]
    pending = [row for row in rows(db) if row["state"] == "PENDING"]
    due = datetime.fromisoformat(pending[0]["next_attempt_at"])
    assert 1.2 <= (due - clock.now()).total_seconds() <= 2.8
    assert all(row["attempts"] == 0 and row["next_attempt_at"] == pending[0]["next_attempt_at"] for row in pending)
    restarted = Outbox(db, clock)
    await restarted.dispatch_once({"qq": adapter}, budget=0)
    assert len(adapter.sent) == 1
    for _ in range(2):
        next_row = next(row for row in rows(db) if row["state"] == "PENDING")
        clock.advance((datetime.fromisoformat(next_row["next_attempt_at"]) - clock.now()).total_seconds())
        await restarted.dispatch_once({"qq": adapter}, budget=0)
    assert [item.delivery_id for item, _ in adapter.sent] == ids
    assert "".join(item.text for item, _ in adapter.sent) == plain_text(text)
    assert all(row["state"] == "SENT" for row in rows(db))


async def test_delayed_conversation_does_not_block_other_conversation(db, clock, target):
    service = Outbox(db, clock)
    enqueue(service, db, target, "第一段" * 400 + "\n\n" + "第二段" * 400, "multi")
    other = target.model_copy(update={"remote_conversation_id": "b"})
    enqueue(service, db, other, "立即发送给另一会话", "other")
    adapter = Adapter(clock)
    await service.dispatch_once({"qq": adapter})
    assert {item.target.remote_conversation_id for item, _ in adapter.sent} == {"a", "b"}
    assert adapter.sent[0][1] == adapter.sent[1][1] == clock.now()
    assert any(row["state"] == "PENDING" and row["next_attempt_at"] for row in rows(db))


async def test_inflight_send_blocks_only_same_conversation_and_gap_starts_after_receipt(db, clock, target):
    service, adapter = Outbox(db, clock), Adapter(clock)
    adapter.release = asyncio.Event()
    enqueue(service, db, target, "first", "1")
    enqueue(service, db, target, "second", "2")
    other = target.model_copy(update={"remote_conversation_id": "b"})
    enqueue(service, db, other, "other", "3")
    running = asyncio.create_task(service.dispatch_once({"qq": adapter}))
    try:
        await asyncio.wait_for(adapter.started.wait(), 2)
        clock.advance(10)
        await Outbox(db, clock).dispatch_once({"qq": adapter})
        assert [item.text for item, _ in adapter.sent].count("first") == 1
        assert "second" not in [item.text for item, _ in adapter.sent]
        assert "other" in [item.text for item, _ in adapter.sent]
        adapter.release.set()
        await asyncio.wait_for(running, 2)
        pending = next(row for row in rows(db) if row["state"] == "PENDING")
        assert datetime.fromisoformat(pending["next_attempt_at"]) > clock.now()
        clock.advance(3)
        await service.dispatch_once({"qq": adapter})
        assert [item.text for item, _ in adapter.sent].count("second") == 1
    finally:
        adapter.release.set()
        await asyncio.gather(running, return_exceptions=True)


async def test_recovered_unknown_attempt_preserves_spacing_without_resending(db, clock, target):
    service = Outbox(db, clock)
    first = enqueue(service, db, target, "already attempted", "first")[0]
    second = enqueue(service, db, target, "new message", "second")[0]
    with db.transaction() as connection:
        connection.execute(update(outbox).where(outbox.c.id == first).values(state="SENDING", attempts=1,
                                                                            attempted_at=clock.now().isoformat()))
    restarted, adapter = Outbox(db, clock), Adapter(clock)
    restarted.recover()
    await restarted.dispatch_once({"qq": adapter})
    assert not adapter.sent
    pending = next(row for row in rows(db) if row["id"] == second)
    assert pending["state"] == "PENDING" and pending["attempts"] == 0 and pending["next_attempt_at"]
    clock.advance(3)
    await restarted.dispatch_once({"qq": adapter})
    assert [item.text for item, _ in adapter.sent] == ["new message"]
    assert next(row for row in rows(db) if row["id"] == first)["state"] == "UNKNOWN"


async def test_budget_zero_keeps_all_messages_deliverable(db, clock, target):
    service, adapter = Outbox(db, clock), Adapter(clock)
    for index in range(8):
        enqueue(service, db, target, "message " + str(index), str(index))
    for _ in range(8):
        await service.dispatch_once({"qq": adapter}, budget=0)
        clock.advance(3)
    assert len(adapter.sent) == 8
    assert all(row["state"] == "SENT" and row["attempts"] == 1 for row in rows(db))
