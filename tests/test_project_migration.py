"""An actual pre-project database retains its input and dialogue identities on upgrade."""
import sqlite3

from alembic import command
from alembic.config import Config

from qqbot.cli import migrate


def test_project_upgrade_preserves_0021_dialogues_and_pending_inputs(tmp_path):
    path = tmp_path / 'previous.db'
    config = Config('alembic.ini')
    config.set_main_option('sqlalchemy.url', 'sqlite:///' + str(path))
    command.upgrade(config, '0021')
    with sqlite3.connect(path) as conn:
        # Older migrations import current metadata. Remove the new columns to reproduce the deployed schema.
        for table, column in [('dialogues', 'workspace_id'), ('task_inputs', 'source_event_id')]:
            if column in {row[1] for row in conn.execute(f'PRAGMA table_info({table})')}:
                conn.execute(f'ALTER TABLE {table} DROP COLUMN {column}')
        conn.execute("INSERT INTO dialogues (id,owner_id,conversation_id,title,session_id,binding_json,state,created_at) "
            "VALUES ('original','owner','conversation','existing','session','{}','ACTIVE','2026-11-13T00:00:00+00:00')")
        conn.execute("INSERT INTO task_inputs (id,task_id,source_id,text,state,created_at) "
            "VALUES ('pending','task','legacy-source','keep this answer','PENDING','2026-11-13T00:00:00+00:00')")
    migrate(str(path), backup=False)
    migrate(str(path), backup=False)
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT id,session_id,workspace_id FROM dialogues').fetchone() == (
            'original', 'session', 'personal')
        assert conn.execute('SELECT id,source_id,source_event_id,text,state FROM task_inputs').fetchone() == (
            'pending', 'legacy-source', None, 'keep this answer', 'PENDING')
        assert conn.execute('SELECT count(*) FROM schedules').fetchone() == (0,)
        assert conn.execute('PRAGMA integrity_check').fetchone() == ('ok',)


def test_synthetic_mail_migration_preserves_preexisting_timer(tmp_path):
    path = tmp_path / 'prior-schedules.db'
    config = Config('alembic.ini')
    config.set_main_option('sqlalchemy.url', 'sqlite:///' + str(path))
    command.upgrade(config, '0023')
    with sqlite3.connect(path) as conn:
        for name in ('synthetic_mail_last_uid', 'synthetic_mail_activation_uid'):
            if name in {row[1] for row in conn.execute('PRAGMA table_info(schedules)')}:
                conn.execute(f'ALTER TABLE schedules DROP COLUMN {name}')
        conn.execute("INSERT INTO schedules (id,owner_id,principal,conversation,creation_key,creation_json,"
            "event_json,dialogue_id,spec_json,revision,enabled,next_fire_at,created_at,updated_at) "
            "VALUES ('timer','owner','principal','conversation','key','{}','{}','dialogue','{}',1,1,"
            "'2030-01-01T00:00:00+00:00','now','now')")
    migrate(str(path), backup=False)
    migrate(str(path), backup=False)
    with sqlite3.connect(path) as conn:
        row = conn.execute('SELECT id, enabled, next_fire_at, synthetic_mail_last_uid, '
            'synthetic_mail_activation_uid FROM schedules').fetchone()
        assert row == ('timer', 1, '2030-01-01T00:00:00+00:00', None, None)
        assert conn.execute('SELECT count(*) FROM synthetic_mail_receipts').fetchone() == (0,)
