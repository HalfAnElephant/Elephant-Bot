"""Small administrative projections of durable queues, usage and subsystem health."""
import json
import math
import os
from datetime import UTC, datetime

from sqlalchemy import func, select

from qqbot.storage.action_schema import operations
from qqbot.storage.agent_schema import agent_events, dialogue_inputs, leader_inbox
from qqbot.storage.learning_schema import learning_sessions
from qqbot.storage.memory_sync_schema import memory_operations, memory_owners, memory_streams
from qqbot.storage.schema import audit_events, outbox
from qqbot.storage.task_schema import model_requests, notifications, processes, runs, tasks

from .task_graph import TERMINAL


def parse_timestamp(value):
    if not value:
        return None
    try:
        timestamp = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    # Imported pre-Agent records may contain dates or UTC times without an offset.
    return timestamp.replace(tzinfo=UTC) if timestamp.tzinfo is None else timestamp


def age_seconds(now, timestamp):
    timestamp = parse_timestamp(timestamp)
    return max(0, (now - timestamp).total_seconds()) if timestamp else None


def runtime_snapshot(core, background):
    now = core.clock.now()
    with core.db.engine.connect() as conn:
        def states(table, column, *conditions):
            return dict(conn.execute(select(column, func.count()).select_from(table)
                .where(*conditions).group_by(column)).all())

        pending_inputs = conn.execute(select(dialogue_inputs.c.dialogue_id,
            func.count().label('pending'), func.min(dialogue_inputs.c.created_at).label('oldest'))
            .where(dialogue_inputs.c.handled_at.is_(None)).group_by(dialogue_inputs.c.dialogue_id)
            .order_by(func.min(dialogue_inputs.c.created_at)).limit(100)).mappings().all()
        inbox = conn.execute(select(func.count(), func.min(leader_inbox.c.created_at))
            .where(leader_inbox.c.handled_at.is_(None))).one()
        goals = conn.execute(select(tasks.c.id, tasks.c.state, tasks.c.created_at).where(
            tasks.c.kind == 'goal', tasks.c.state.not_in(TERMINAL))).mappings().all()
        children = conn.execute(select(tasks.c.root_task_id, tasks.c.state).where(
            tasks.c.kind != 'goal', tasks.c.root_task_id.in_([row['id'] for row in goals]))).all()
        active_children = {root_id for root_id, state in children if state not in TERMINAL}
        pending_verification = [row['id'] for row in goals
            if row['state'] == 'WAITING_CHILDREN' and row['id'] not in active_children]
        completed = select(func.max(agent_events.c.created_at).label('completed_at')).join(
            tasks, tasks.c.id == agent_events.c.task_id).where(tasks.c.root_task_id.in_(pending_verification),
            agent_events.c.kind == 'child_result').group_by(tasks.c.root_task_id).subquery()
        oldest_result = conn.execute(select(func.min(completed.c.completed_at))).scalar_one_or_none()
        latencies = conn.execute(select(dialogue_inputs.c.created_at, dialogue_inputs.c.consumed_at)
            .where(dialogue_inputs.c.consumed_at.is_not(None))
            .order_by(dialogue_inputs.c.created_at.desc()).limit(1000)).all()
        latency_seconds = []
        for created, consumed in latencies:
            finished = parse_timestamp(consumed)
            elapsed = age_seconds(finished, created) if finished else None
            if elapsed is not None:
                latency_seconds.append(elapsed)
        latency_seconds.sort()
        usage = states(model_requests, model_requests.c.kind)
        unsettled = states(operations, operations.c.state,
            operations.c.state.in_(['DISPATCHED', 'UNKNOWN', 'PARTIAL', 'RECONCILIATION']))
        delivery = states(outbox, outbox.c.state)
        memory = states(memory_owners, memory_owners.c.state)
        memory_queue = states(memory_operations, memory_operations.c.state)
        watermarks = [dict(row) for row in conn.execute(select(memory_streams.c.scope_id, memory_streams.c.session_id,
            memory_streams.c.projection_version, memory_streams.c.epoch, memory_streams.c.exported_seq,
            memory_streams.c.extracted_seq, memory_streams.c.indexed_seq)
            .order_by(memory_streams.c.epoch.desc(), memory_streams.c.id).limit(100)).mappings()]
        review_pending = conn.execute(select(func.count()).select_from(learning_sessions)
            .where(learning_sessions.c.pending_json.is_not(None))).scalar_one()
        skill_failures = conn.execute(select(func.count()).select_from(audit_events)
            .where(audit_events.c.kind == 'skills.write_failed')).scalar_one()
        agents = states(runs, runs.c.role, runs.c.slot_reserved == 1)
        resident = states(processes, processes.c.state)
        wakeups = conn.execute(select(func.count()).select_from(leader_inbox)).scalar_one()
        final_notifications = conn.execute(select(func.count()).select_from(notifications)
            .where(notifications.c.purpose == 'result')).scalar_one()
        latest_progress = conn.execute(select(func.max(agent_events.c.created_at))
            .where(agent_events.c.kind.in_(['milestone', 'finished', 'child_result']))).scalar_one_or_none()
        request_states = states(model_requests, model_requests.c.state, model_requests.c.kind != 'tool')
        usage_samples = conn.execute(select(model_requests.c.usage_json).where(
            model_requests.c.kind != 'tool', model_requests.c.state == 'COMPLETED')
            .order_by(model_requests.c.created_at.desc()).limit(1000)).scalars().all()
    usage_values = [json.loads(raw) if raw else None for raw in usage_samples]
    reported = sum(isinstance(value, dict) and value.get('coverage') == 'reported' for value in usage_values)
    scheduler = background.get('loops', {}).get('scheduler', {})
    goal_ages = [age_seconds(now, row['created_at']) for row in goals]
    return {
        'agent_ready': bool(background.get('healthy') and core.settings.model.endpoint and core.settings.model.model
            and os.environ.get(core.settings.model.api_key_env)),
        'scheduler_ready': bool(background.get('running') and scheduler.get('healthy', not background.get('enabled'))),
        'delivery_status': {'healthy': background.get('loops', {}).get('delivery', {}).get('healthy', not background.get('enabled')),
            'states': delivery},
        'memory_status': {'enabled': bool(core.settings.memory.openviking.endpoint), 'owners': memory,
            'operations': memory_queue, 'watermarks': watermarks},
        'skills_status': {'enabled': core.settings.skills.enabled, 'provider': core.settings.skills.provider,
            'review_pending': review_pending, 'recorded_write_failures': skill_failures},
        'runtime': {
            'agents_active_by_role': agents, 'resident_processes': resident,
            'sessions_queued': [{'dialogue_id': row['dialogue_id'], 'pending_inputs': row['pending'],
                'oldest_input_seconds': age_seconds(now, row['oldest'])} for row in pending_inputs],
            'goals_by_state': {state: sum(row['state'] == state for row in goals) for state in {row['state'] for row in goals}},
            'oldest_goal_seconds': max((age for age in goal_ages if age is not None), default=None),
            'leader_inbox_pending': inbox[0], 'leader_inbox_oldest_seconds': age_seconds(now, inbox[1]),
            'goals_awaiting_verification': len(pending_verification),
            'oldest_verification_seconds': age_seconds(now, oldest_result),
            'last_effective_progress_at': latest_progress,
            'input_consumption_latency': {'samples': len(latency_seconds),
                'p95_seconds': latency_seconds[math.ceil(len(latency_seconds) * .95) - 1] if latency_seconds else None},
            'calls': {'model': sum(count for kind, count in usage.items() if kind != 'tool'),
                'tool': usage.get('tool', 0), 'by_kind': usage, 'model_request_states': request_states},
            'usage_coverage': {'samples': len(usage_samples), 'reported': reported,
                'unknown': len(usage_samples) - reported},
            'unsettled_operations': unsettled, 'leader_events_recorded': wakeups,
            'final_logical_notifications': final_notifications,
        },
    }
