"""Expression evidence remains revocable across prompts, administration and recovery."""
import json

from fastapi.testclient import TestClient

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.expressions import ExpressionService
from qqbot.expressions.learner import LearnedExpression, LearningResult
from qqbot.storage.db import Database
from qqbot.storage.operations import backup, export_deletions, restore


class FixtureLearner:
    configured = True

    async def learn(self, events, *, self_reflect=True):
        return LearningResult(state="DONE", candidates=tuple(
            LearnedExpression(situation=f"惊讶的场景{index}", style=f"这也太离谱{index}", source_event_id=event.event_id)
            for index, event in enumerate(events[:10])
        ))


async def populated_core(tmp_path, nap_config, nap_event, clock):
    app = create_app(Settings(database=str(tmp_path / "core.db"), adapters=(nap_config,)),
                     clock=clock, run_background=False)
    core = app.state.core
    events = []
    for index in range(10):
        payload = {**nap_event, "message_id": 8000 + index,
                   "message": [{"type": "text", "data": {"text": f"这也太离谱了，第{index}次遇到了"}}]}
        events.append(core.inbox.accept(core.adapters["nap"].normalize(payload))[0])
    access = core.access(events[-1])
    result = await core.expressions.maintenance_once(access, FixtureLearner())
    assert result["state"] == "DONE"
    assert len(core.expressions.list(access)) == 10
    return app, core, access, events


async def test_expression_context_revocation_and_restore(tmp_path, nap_config, nap_event, clock):
    _, core, access, events = await populated_core(tmp_path, nap_config, nap_event, clock)
    try:
        turn = core.contexts.turn(events[-1])
        context = core.contexts.build(access, turn)
        assert context["seed"]["surface"] == "group"
        assert "expression_habits" not in context["seed"]
        assert "你可以视情况自由使用以下句式" in context["expression_habits"]
        manifest = [item for item in context["source_manifest"] if item["kind"] == "expression"]
        assert len(manifest) == 5
        assert core.validate_context(access, context)
        snapshot = tmp_path / "before-expression-delete.sqlite"
        backup(core.db.path, snapshot)
        deleted = manifest[0]
        core.expressions.delete(access.model_copy(update={"role": "owner"}), deleted["id"],
                                expected_revision=deleted["revision"])
        assert not core.validate_context(access, context)
        ledger = tmp_path / "expression-deletions.json"
        export_deletions(core.db.path, ledger)
        assert deleted["id"] in {item["expression_id"] for item in json.loads(ledger.read_text())["expression_tombstones"]}
        destination = tmp_path / "restored.sqlite"
        restore(snapshot, destination, ledger)
        recovered = Database(str(destination))
        try:
            service = ExpressionService(recovered, clock, core.settings.expression, permissions=core.permissions)
            assert deleted["id"] not in {row["id"] for row in service.list(access)}
            assert not service.validate_record(access, deleted["id"], deleted["revision"])
        finally:
            recovered.close()
        record = next(row for row in core.expressions.list(access) if events[-1].event_id not in row["source_event_ids"])
        core.social.revoke_source(access, record["source_event_ids"][0])
        assert not core.expressions.validate_record(access, record["id"], record["revision"])
        fresh = core.contexts.build(access, turn)
        assert fresh["expression_habits"] == ""  # Fewer than ten usable expressions: MaiBot cold-start rule.
        assert core.validate_context(access, fresh)
    finally:
        await core.stop()
        core.db.close()


async def test_expression_admin_scope_and_revision(tmp_path, nap_config, nap_event, clock, monkeypatch):
    monkeypatch.setenv("QQBOT_ADMIN_TOKEN", "test-admin")
    app, core, access, _ = await populated_core(tmp_path, nap_config, nap_event, clock)
    auth = {"Authorization": "Bearer test-admin"}
    query = "?conversation_id=" + access.conversation.key
    try:
        with TestClient(app) as client:
            assert client.get("/api/v1/expressions" + query).status_code == 401
            response = client.get("/api/v1/expressions" + query, headers=auth)
            assert response.status_code == 200
            assert not response.json()["learner_configured"]
            record = response.json()["records"][0]
            url = "/api/v1/expressions/" + record["id"] + query
            body = {"expected_revision": record["revision"], "checked": True, "style": "怎么还能这样"}
            revised = client.patch(url, headers=auth, json=body)
            assert revised.status_code == 200 and revised.json()["checked"]
            assert client.patch(url, headers=auth, json=body).status_code == 409
            assert not core.expressions.validate_record(access, record["id"], record["revision"])
            assert client.delete(url + "&expected_revision=" + str(revised.json()["revision"]), headers=auth).status_code == 200
    finally:
        await core.stop()
        core.db.close()


async def test_custom_expression_endpoint_does_not_receive_primary_key(tmp_path, monkeypatch):
    from qqbot.expressions.config import ExpressionConfig

    monkeypatch.setenv("QQBOT_MODEL_KEY", "primary-secret")
    app = create_app(Settings(database=str(tmp_path / "core.db"),
                             expression=ExpressionConfig(learner_endpoint="https://other.invalid/chat", learner_model="other")),
                     run_background=False)
    assert app.state.core.expression_learner.api_key is None
    await app.state.core.stop()
    app.state.db.close()
