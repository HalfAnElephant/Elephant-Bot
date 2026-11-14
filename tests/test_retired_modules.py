from pathlib import Path

from qqbot import context, dialogue, expressions


def test_retired_social_and_persona_entrypoints_stay_absent():
    package = Path(__file__).resolve().parents[1] / "src" / "qqbot"
    for module in (
        "policy/__init__.py",
        "policy/judge.py",
        "domain/social.py",
        "conversation/__init__.py",
        "speakup.py",
    ):
        assert not (package / module).exists(), module

    assert not hasattr(context, "ContextService")
    assert not hasattr(dialogue, "PersonaStore")
    assert not hasattr(dialogue, "assemble_prompt")
    assert not hasattr(expressions, "ExpressionService")
    assert not hasattr(expressions, "build_habits")
    assert not (package.parents[1] / "evals" / "social_replay.py").exists()


def test_legacy_expression_deletion_keeps_tombstones(db, clock):
    import json

    from sqlalchemy import insert, select

    from qqbot.domain import stable_id
    from qqbot.storage.expression_schema import expression_tombstones, expression_versions
    from qqbot.storage.expression_schema import expressions as records

    now = clock.now().isoformat()
    with db.transaction() as conn:
        conn.execute(insert(records).values(id="legacy", conversation_id="conversation", surface="group",
            situation="old situation", style="old style", count=1, checked=True, modified_by="user",
            status="active", revision=1, sources_json=json.dumps([{"id": "source"}]),
            created_at=now, updated_at=now))
        expressions.invalidate_source(conn, "source", now=clock.now())
        expressions.invalidate_source(conn, "source", now=clock.now())
        row = conn.execute(select(records)).mappings().one()
        assert (row["status"], row["situation"], row["style"], row["sources_json"], row["revision"]) == (
            "deleted", "", "", "[]", 2)
        assert set(conn.execute(select(expression_tombstones.c.expression_id)).scalars()) == {
            "legacy", stable_id("maibot-expression", "conversation", "old situation", "old style")}
        assert conn.execute(select(expression_versions.c.version)).scalar_one() == 1
