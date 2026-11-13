"""Durable QQ stream intent and outcome; never replay an uncertain network write."""
from sqlalchemy import Column, Integer, String, Table, Text, UniqueConstraint

from qqbot.storage.schema import metadata

stream_runs = Table(
    "qq_stream_runs", metadata,
    Column("run_id", String, primary_key=True),
    Column("task_id", String),
    Column("state", String, nullable=False),
    Column("cursor_seq", Integer, nullable=False, server_default="0"),
    Column("error", String),
    Column("result_origin_kind", String),
    Column("result_origin_id", String),
)
stream_messages = Table(
    "qq_stream_messages", metadata,
    Column("id", String, primary_key=True),
    Column("run_id", String, nullable=False),
    Column("message_key", String, nullable=False),
    Column("kind", String, nullable=False, server_default="answer"),
    Column("state", String, nullable=False),
    Column("text", Text, nullable=False, server_default=""),
    Column("sent_text", Text, nullable=False, server_default=""),
    Column("stream_id", String),
    Column("next_index", Integer, nullable=False, server_default="0"),
    Column("remain_msg_len", Integer),
    Column("final_marker", Integer, nullable=False, server_default="0"),
    Column("envelope_json", Text, nullable=False),
    Column("error", String),
    Column("created_at", String, nullable=False),
    UniqueConstraint("run_id", "message_key", "kind"),
)
