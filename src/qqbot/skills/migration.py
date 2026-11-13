"""One-time export of successful, active legacy candidates; never writes legacy bodies."""
import hashlib
import json

from sqlalchemy import select

from qqbot.adapters.hermes_skills._vendor.host import atomic_write_text
from qqbot.storage.skill_schema import learned_skills
from qqbot.storage.task_schema import runs, tasks


def _markdown(document):
    import yaml
    description = document['description']
    frontmatter = yaml.safe_dump({'name': document['name'], 'description': description[:60]},
                                allow_unicode=True, sort_keys=False).strip()
    sections = [('When to use', [document['when_to_use']]), ('Description', [description]),
                ('Steps', document['steps']), ('Verification', document['verification']),
                ('Pitfalls', document.get('pitfalls', []))]
    return '---\n' + frontmatter + '\n---\n\n' + '\n\n'.join(
        '## ' + title + '\n' + '\n'.join('- ' + item for item in items)
        for title, items in sections if items) + '\n'


def migrate_legacy_skills(db, provider_for_scope):
    """Caller freezes old writes first and supplies explicit, verified legacy scope→owner mapping."""
    with db.engine.connect() as conn:
        rows = conn.execute(select(learned_skills, tasks.c.state.label('task_state'),
                                   runs.c.result_json.label('source_result'))
                            .join(tasks, learned_skills.c.task_id == tasks.c.id)
                            .join(runs, learned_skills.c.run_id == runs.c.id)
                            .order_by(learned_skills.c.version.desc())).mappings().all()
    manifest, newest = [], set()
    for row in rows:
        key = (row['scope'], row['name'])
        item = {key: row[key] for key in ('id', 'scope', 'name', 'version', 'task_id', 'state', 'fingerprint')}
        item['document_sha256'] = hashlib.sha256(row['document_json'].encode()).hexdigest()
        manifest.append(item)
        if key in newest:
            item['migration'] = 'historical_version'
            continue
        if row['state'] == 'archived':
            newest.add(key)
            item['migration'] = 'archived_not_imported'
            continue
        if row['task_state'] != 'SUCCEEDED' or json.loads(row['source_result'] or '{}').get('status') != 'completed':
            item['migration'] = 'unsuccessful_not_imported'
            continue
        newest.add(key)
        adapter = provider_for_scope(row['scope'])
        if adapter is None:
            item['migration'] = 'unbound_scope'
            continue
        document = json.loads(row['document_json'])
        content = _markdown(document)
        name = document['name']
        existing = adapter.skill_view({'name': name})
        if existing.get('success') and existing.get('content') != content:
            # Same owner can have several formerly conversation-local skills with the same name.
            name = name[:50] + '-' + item['document_sha256'][:12]
            document['name'] = name
            content = _markdown(document)
            existing = adapter.skill_view({'name': name})
        if existing.get('success'):
            result = {'success': existing.get('content') == content}
        else:
            result = adapter.skill_manage({'operations': [{'action': 'create', 'name': name, 'content': content}]})
        item.update(migration='imported' if result.get('success') else 'failed', target_name=name)
        if not result.get('success'):
            item['error'] = result.get('error', 'Existing skill content differs.')
    # Per-owner audit contains hashes and source IDs, never a second writable skill body.
    owners = {}
    for item in manifest:
        adapter = provider_for_scope(item['scope'])
        if adapter is not None:
            owners.setdefault(adapter.root, (adapter, []))[1].append(item)
    for adapter, entries in owners.values():
        with adapter.context():
            atomic_write_text(adapter.root / 'legacy-skills-migration.json',
                              json.dumps(entries, ensure_ascii=False, indent=2) + '\n')
    return manifest
