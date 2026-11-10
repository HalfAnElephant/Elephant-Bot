"""Saved, Core and Worker configuration identities for restart diagnostics."""

import hashlib
import json
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert

from qqbot.configuration import changes
from qqbot.harness.skills import DEFAULT_SKILLS, skill_files, skill_version
from qqbot.storage.schema import audit_events


def model_identity(model, endpoint, context_window):
    values = {'model': model, 'endpoint': endpoint, 'context_window': context_window}
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


def report_worker(service, worker_id, payload):
    if set(payload) != {'model_identity', 'skill_version', 'loaded_skill_versions'}:
        raise ValueError('invalid worker configuration report')
    loaded = payload['loaded_skill_versions']
    if not isinstance(loaded, list) or len(loaded) > 33:
        raise ValueError('invalid loaded skill versions')
    versions = [payload['model_identity'], payload['skill_version'], *loaded]
    if len(versions) > 35 or any(
        not isinstance(value, str) or len(value) != 64 or any(ch not in '0123456789abcdef' for ch in value)
        for value in versions
    ):
        raise ValueError('invalid worker configuration fingerprint')
    row = {'id': 'worker-config:' + worker_id, 'kind': 'worker.config', 'target': worker_id, 'actor': 'worker',
           'created_at': service.clock.now().isoformat(), 'detail': json.dumps(payload)}
    with service.db.transaction() as c:
        c.execute(insert(audit_events).values(**row).on_conflict_do_update(
            index_elements=['id'], set_={key: row[key] for key in ('created_at', 'detail')}))
    return {'recorded': True}


def configuration_status(core, saved):
    def identity(settings):
        return model_identity(settings.model.model, settings.model.endpoint, settings.model.context_window)
    expected_model, core_model = identity(saved), identity(core.settings)
    skill_error = None
    try:
        expected_skills = skill_version(skill_files(DEFAULT_SKILLS))
    except (OSError, ValueError) as error:
        expected_skills, skill_error = None, type(error).__name__
    with core.db.engine.connect() as c:
        rows = c.execute(select(audit_events).where(audit_events.c.kind == 'worker.config')).mappings().all()
    workers = []
    for row in rows:
        loaded = json.loads(row['detail'])
        stale = (core.clock.now() - datetime.fromisoformat(row['created_at'])).total_seconds() > 90
        workers.append({'worker_id': row['target'], 'reported_at': row['created_at'], 'stale': stale,
                        **loaded, 'model_matches': loaded['model_identity'] == expected_model,
                        'skills_match': loaded['skill_version'] == expected_skills})
    restart = []
    if changes(core.settings, saved)["restart_required"] or expected_model != core_model:
        restart.append('Core')
    if any(not worker['model_matches'] for worker in workers):
        restart.append('Worker')
    return {'expected_model': expected_model, 'core_loaded_model': core_model,
            'expected_skills': expected_skills, 'skills_error': skill_error,
            'workers': workers, 'worker_status': ('stale' if all(worker['stale'] for worker in workers) else 'reported') if workers else 'unknown',
            'restart_components': restart}
