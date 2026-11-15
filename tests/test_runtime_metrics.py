"""Admin status reports durable queues and actual reservations without model calls."""
import json

from fastapi.testclient import TestClient
from sqlalchemy import insert, select, update

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.skills import record_skill_result
from qqbot.storage.action_schema import operations
from qqbot.storage.task_schema import tasks


def test_runtime_status_projects_queues_calls_failures_and_unknowns(tmp_path, clock, monkeypatch):
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'metrics-token')
    settings = Settings(database=str(tmp_path / 'metrics.db'))
    app = create_app(settings, clock=clock, run_background=False)
    core = app.state.core
    headers = {'Authorization': 'Bearer metrics-token'}
    with TestClient(app) as client:
        accepted = core.enqueue_admin_task('核对项目事实', idempotency_key='metrics-goal')
        clock.advance(5)
        claim = core.tasks.claim('metrics-worker')
        identity = (claim['run_id'], claim['fencing_token'], 'metrics-worker')
        core.tasks.reserve_budget(*identity, {'request_id': 'tool-one', 'kind': 'tool'})
        core.tasks.reserve_budget(*identity, {'request_id': 'model-one', 'kind': 'business'})
        core.tasks.record_usage(*identity, {'request_id': 'model-one', 'usage': {'coverage': 'reported'}})
        core.tasks.reserve_budget(*identity, {'request_id': 'model-null', 'kind': 'business'})
        core.tasks.record_usage(*identity, {'request_id': 'model-null', 'usage': None})
        record_skill_result(core.tasks, claim['run_id'], 'skill-call', {'success': False, 'error': 'private body'})
        record_skill_result(core.tasks, claim['run_id'], 'skill-call', {'success': False})
        with core.db.transaction() as conn:
            conn.execute(insert(operations).values(id='unknown', task_id=claim['task_id'], run_id=claim['run_id'],
                fence=claim['fencing_token'], arguments_hash='a' * 64, action_json='{}', arguments_json='{}',
                state='UNKNOWN', result_json='{}', updated_at=clock.now().isoformat()))
        assert client.get('/api/v1/status').status_code == 401
        response = client.get('/api/v1/status', headers=headers)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body['skills_status']['recorded_write_failures'] == 1
        assert body['runtime']['calls']['tool'] == 1
        assert body['runtime']['calls']['model'] == 2
        assert body['runtime']['usage_coverage'] == {'samples': 2, 'reported': 1, 'unknown': 1}
        assert body['runtime']['unsettled_operations'] == {'UNKNOWN': 1}
        assert body['runtime']['oldest_goal_seconds'] == 5
        assert body['runtime']['agents_active_by_role'] == {'worker': 1}
        assert body['runtime']['leader_inbox_pending'] == 0
        assert 'private body' not in response.text
        with core.db.engine.connect() as conn:
            root = conn.execute(select(tasks).where(tasks.c.id == accepted['task_id'])).mappings().one()
        assert json.loads(root['budget_json'])['model_calls'] == 2


def test_runtime_status_handles_empty_queues_and_usage(tmp_path, clock, monkeypatch):
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'metrics-token')
    app = create_app(Settings(database=str(tmp_path / 'empty.db')), clock=clock, run_background=False)
    with TestClient(app) as client:
        body = client.get('/api/v1/status', headers={'Authorization': 'Bearer metrics-token'}).json()
        assert body['runtime']['leader_inbox_oldest_seconds'] is None
        assert body['runtime']['oldest_verification_seconds'] is None
        assert body['runtime']['input_consumption_latency'] == {'samples': 0, 'p95_seconds': None}
        assert body['runtime']['usage_coverage'] == {'samples': 0, 'reported': 0, 'unknown': 0}


def test_runtime_status_accepts_legacy_utc_dates(tmp_path, clock, monkeypatch):
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'metrics-token')
    app = create_app(Settings(database=str(tmp_path / 'legacy.db')), clock=clock, run_background=False)
    with TestClient(app) as client:
        app.state.core.enqueue_admin_task('旧任务', idempotency_key='legacy')
        with app.state.db.transaction() as conn:
            conn.execute(update(tasks).values(created_at='2026-11-01'))
        response = client.get('/api/v1/status', headers={'Authorization': 'Bearer metrics-token'})
        assert response.status_code == 200, response.text
        assert response.json()['runtime']['oldest_goal_seconds'] >= 0
