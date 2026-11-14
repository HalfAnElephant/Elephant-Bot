"""Authenticated host entry to Hermes skills; legacy database bodies are read-only."""
import hashlib
import json

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from qqbot.adapters.hermes_skills import GUIDANCE
from qqbot.domain import stable_id
from qqbot.skills.config import SkillsConfig
from qqbot.storage.schema import audit_events

__all__ = ["GUIDANCE", "record_skill_result", "skill_operation"]



def record_skill_result(service, run_id, call_id, result):
    """Record failed writes once without retaining skill bodies or upstream error text."""
    if not isinstance(result, dict) or not (result.get("success") is False or result.get("error")):
        return
    with service.db.transaction() as conn:
        conn.execute(sqlite_insert(audit_events).values(
            id=stable_id("skill-write-failed", run_id, call_id), kind="skills.write_failed",
            target=run_id, actor="harness", created_at=service.clock.now().isoformat(),
            detail=json.dumps({"run_id": run_id, "error_type": "SkillManageRejected"}))
            .on_conflict_do_nothing(index_elements=[audit_events.c.id]))

def skill_operation(service, args, payload):
    if (not isinstance(payload, dict) or not {'operation', 'arguments'} <= set(payload)
            or set(payload) - {'operation', 'arguments', 'call_id'}):
        raise ValueError('Legacy candidate skills API retired; use skills_list, skill_view or skill_manage.')
    if 'call_id' in payload and (not isinstance(payload['call_id'], str) or not 1 <= len(payload['call_id']) <= 200):
        raise ValueError('Invalid skill call identity.')
    name, arguments = payload['operation'], payload['arguments']
    if name not in {'skills_list', 'skill_view', 'skill_manage'}:
        raise ValueError('Legacy candidate skills API retired; use native Hermes skill tools.')
    action = 'skills.write' if name == 'skill_manage' else 'skills.read'
    service.authorize(*args, {'action': action})
    with service.db.engine.connect() as conn:
        run, task = service._lease(conn, *args)
    access = service._access_for(task)
    settings = getattr(service, 'skills_settings', None)
    settings = settings() if callable(settings) else settings or SkillsConfig()
    if not settings.enabled or (access.conversation.surface != 'private' and not settings.group_enabled):
        raise PermissionError('Skills are disabled for this conversation.')
    service.permissions.require(access, action, 'conversation:' + access.conversation.key)
    provider = getattr(service, 'skills_provider', None)
    if provider is None:
        raise RuntimeError('Owner-bound Hermes skills provider is not configured.')
    adapter = provider(access, workspace_id=json.loads(task["context_json"]).get("workspace_id", "personal"))
    if adapter is None:
        raise PermissionError('No owner skill scope is bound to this identity.')
    result = adapter.call(name, arguments, task_id=task['id'], session_id=run.get('session_id') or run['id'])
    if name == 'skill_manage':
        call_id = payload.get('call_id') or hashlib.sha256(json.dumps(arguments, sort_keys=True).encode()).hexdigest()
        record_skill_result(service, run['id'], call_id, result)
    return result
