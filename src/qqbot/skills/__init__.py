"""Authenticated host entry to Hermes skills; legacy database bodies are read-only."""
from qqbot.adapters.hermes_skills import GUIDANCE
from qqbot.skills.config import SkillsConfig

__all__ = ["GUIDANCE", "skill_operation"]


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
    adapter = provider(access)
    if adapter is None:
        raise PermissionError('No owner skill scope is bound to this identity.')
    return adapter.call(name, arguments, task_id=task['id'], session_id=run.get('session_id') or run['id'])
