import json
import random

import httpx
import pytest
from sqlalchemy import select, update

from qqbot.app import create_app
from qqbot.config import ModelConfig, Settings, SpeakUpConfig
from qqbot.dialogue import EXPRESSION_RULES
from qqbot.domain import OutboundEnvelope
from qqbot.speakup import SpeakUp
from qqbot.storage.schema import outbox


def setup_core(tmp_path, clock, nap_config, monkeypatch, handler):
    monkeypatch.setenv('SPEAKUP_KEY', 'test')
    cfg = nap_config.model_copy(update={'admin_users': ('300',)})
    settings = Settings(database=str(tmp_path / 'core.db'), adapters=(cfg,),
                        model=ModelConfig(endpoint='https://test/model', model='test', api_key_env='SPEAKUP_KEY'),
                        speak_up=SpeakUpConfig(min_interval_minutes=1, max_interval_minutes=1, idle_minutes=1))
    core = create_app(settings, clock=clock, run_background=False,
                      model_client=httpx.AsyncClient(transport=httpx.MockTransport(handler))).state.core
    core.adapters['nap'].account_online = True
    return core


def incoming(core, clock, mid=1):
    event = core.adapters['nap'].normalize({'post_type': 'message', 'message_type': 'private',
        'self_id': 100, 'user_id': 300, 'message_id': mid, 'time': int(clock.now().timestamp()),
        'message': [{'type': 'text', 'data': {'text': '今天吃了米饭'}}]})
    event = core.inbox.accept(event)[0]
    core.inbox.finish(event.event_id)
    return event


def response(text):
    return httpx.Response(200, json={'choices': [{'message': {'content': text}}]})


def deliveries(core):
    with core.db.engine.connect() as c:
        return [OutboundEnvelope.model_validate_json(raw) for raw in c.execute(select(outbox.c.envelope_json)).scalars()]


async def test_schedule_survives_restart_and_output_rechecks_state(tmp_path, clock, nap_config, monkeypatch):
    calls = []
    def handler(request):
        calls.append(json.loads(request.content))
        return response('吃了没？')
    core = setup_core(tmp_path, clock, nap_config, monkeypatch, handler)
    event = incoming(core, clock)
    await core.speak_up.tick()
    assert not calls
    core.speak_up = SpeakUp(core)
    clock.advance(61)
    await core.speak_up.tick()
    sent = deliveries(core)
    assert len(sent) == 1 and len(calls) == 1
    assert 'tools' not in calls[0] and calls[0]['max_tokens'] == 300
    system = calls[0]['messages'][0]['content']
    assert system.count(EXPRESSION_RULES) == 1
    assert core.personas._core.private_chat_prompts in system
    assert core.personas._core.group_chat_prompt not in system
    assert sent[0].allow_proactive and sent[0].reply_context is None
    assert core.pre_send(sent[0])
    core.save_state(event.conversation.key, mode='silent')
    assert not core.pre_send(sent[0])
    core.save_state(event.conversation.key, mode='addressed_only')
    incoming(core, clock, mid=2)
    assert not core.pre_send(sent[0])
    core.speak_up = SpeakUp(core)
    await core.speak_up.tick()
    assert len(calls) == 1  # Pending output and recent input prevent repeated attempts.
    await core.stop()
    core.db.close()


@pytest.mark.parametrize('outcome', ['silence', 'failure', 'new_input'])
async def test_unsent_attempts_are_bounded_and_not_replayed(outcome, tmp_path, clock, nap_config, monkeypatch):
    calls = []
    def handler(request):
        calls.append(request)
        if outcome == 'failure':
            return httpx.Response(503)
        if outcome == 'new_input':
            incoming(core, clock, mid=2)
        return response('[沉默]' if outcome == 'silence' else '来啦')
    core = setup_core(tmp_path, clock, nap_config, monkeypatch, handler)
    core.settings = core.settings.model_copy(update={'speak_up': core.settings.speak_up.model_copy(update={'max_per_day': 1})})
    incoming(core, clock)
    await core.speak_up.tick()
    clock.advance(61)
    await core.speak_up.tick()
    assert len(calls) == 1 and not deliveries(core)
    core.speak_up = SpeakUp(core)
    clock.advance(120)
    await core.speak_up.tick()
    assert len(calls) == 1
    await core.stop()
    core.db.close()


async def test_question_nudge_once_outside_daily_quota_and_quiet_hours(tmp_path, clock, nap_config, monkeypatch):
    calls = []
    def handler(request):
        calls.append(request)
        return response('[沉默]')
    core = setup_core(tmp_path, clock, nap_config, monkeypatch, handler)
    event = incoming(core, clock)
    core.settings = core.settings.model_copy(update={'speak_up': core.settings.speak_up.model_copy(update={'max_per_day': 0})})
    clock.advance(1)
    with core.db.transaction() as c:
        core.outbox.enqueue_text(c, target=event.conversation, text='今天吃啥？', origin_kind='chat', origin_id='reply')
        c.execute(update(outbox).values(state='SENT', sent_at=clock.now().isoformat()))
    core.speak_up = SpeakUp(core, rng=random.Random(1))
    # Deterministic coin toss after initial scheduling.
    monkeypatch.setattr(core.speak_up.rng, 'random', lambda: 0.1)
    clock.advance(16 * 60)
    await core.speak_up.tick()
    assert len(calls) == 1
    assert '今天吃啥' in json.loads(calls[0].content)['messages'][1]['content']
    clock.advance(60)
    await core.speak_up.tick()
    assert len(calls) == 1
    core.settings = core.settings.model_copy(update={'speak_up': core.settings.speak_up.model_copy(update={'awake_from': 9})})
    # 08:00 local time is outside the configured window.
    clock.value = clock.value.replace(hour=0, minute=0)
    core.speak_up = SpeakUp(core)
    await core.speak_up.tick()
    assert len(calls) == 1
    await core.stop()
    core.db.close()


async def test_only_selected_private_targets_with_permission_and_proactive_capability(
    tmp_path, clock, nap_config, monkeypatch,
):
    from qqbot.permissions import PermissionPolicy, PermissionService

    calls = []
    core = setup_core(tmp_path, clock, nap_config, monkeypatch,
                      lambda request: calls.append(request) or response('来啦'))
    event = incoming(core, clock)
    adapter = core.adapters['nap']
    adapter.config = adapter.config.model_copy(update={'admin_users': ()})
    await core.speak_up.tick()
    clock.advance(61)
    await core.speak_up.tick()
    assert not calls
    core.settings = core.settings.model_copy(update={'speak_up': core.settings.speak_up.model_copy(
        update={'allow': (event.conversation.key,)})})
    adapter.account_online = False
    clock.advance(61)
    await core.speak_up.tick()
    assert not calls
    adapter.account_online = True
    saved_permissions = core.permissions
    core.permissions = PermissionService(policy=PermissionPolicy(preset='custom', default_effect='deny'))
    clock.advance(61)
    await core.speak_up.tick()
    assert not calls
    core.permissions = saved_permissions
    clock.advance(61)
    await core.speak_up.tick()  # First eligible scan creates the schedule.
    clock.advance(61)
    await core.speak_up.tick()
    assert len(calls) == 1
    await core.stop()
    core.db.close()
