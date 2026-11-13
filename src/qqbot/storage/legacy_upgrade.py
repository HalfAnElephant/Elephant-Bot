"""Bridge the deployed experimental 0010 without editing either published revision."""
from datetime import UTC, datetime

from sqlalchemy import create_engine, insert, inspect, select

from qqbot.storage.interaction_schema import qq_interaction_events, qq_interaction_tokens
from qqbot.storage.schema import audit_events


def bridge_experimental_0010(path):
    engine = create_engine(f"sqlite:///{path}")
    try:
        with engine.begin() as conn:
            tables = set(inspect(conn).get_table_names())
            if "alembic_version" not in tables:
                return
            revision = conn.exec_driver_sql("SELECT version_num FROM alembic_version").scalar_one_or_none()
            if revision != "0010" or {"qq_interaction_tokens", "qq_interaction_events"} <= tables:
                return
            columns = {column["name"] for column in inspect(conn).get_columns("tasks")}
            if "execution_priority" not in columns:
                raise ValueError("unrecognized 0010 schema; migration requires an explicit compatibility bridge")
            for table in (qq_interaction_tokens, qq_interaction_events):
                table.create(conn, checkfirst=True)
            identifier = "experimental-0010-to-official-bridge-v1"
            if not conn.execute(select(audit_events.c.id).where(audit_events.c.id == identifier)).first():
                conn.execute(insert(audit_events).values(id=identifier, kind="migration.compatibility",
                    actor="host", target="0010", created_at=datetime.now(UTC).isoformat(),
                    detail="Created absent official interaction tables; preserved experimental priority and all task/input/delivery rows."))
    finally:
        engine.dispose()
