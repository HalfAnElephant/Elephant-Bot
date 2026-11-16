import json
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import insert, select

from qqbot.app import create_app
from qqbot.config import Settings
from qqbot.permissions import PermissionPolicy, PermissionRule
from qqbot.storage.mail_send_schema import mail_outgoing


@pytest.fixture
async def delivery_admin(tmp_path, clock, monkeypatch):
    monkeypatch.setenv('QQBOT_ADMIN_TOKEN', 'delivery-admin-token')
    app = create_app(Settings(database=str(tmp_path / 'delivery.db'), owner={'id': 'owner'}),
        clock=clock, run_background=False)
    core = app.state.core
    content = {'from': 'owner@example.org', 'to': ['recipient@example.org'], 'cc': [],
        'bcc': ['hidden@example.org'], 'subject': 'SMTP 回执', 'body': 'PRIVATE_MESSAGE_BODY',
        'attachments': [], 'reply': None}

    def seed(identifier='unknown-mail', *, owner_id='owner', state='UNKNOWN'):
        with core.db.transaction() as conn:
            conn.execute(insert(mail_outgoing).values(id=identifier, owner_id=owner_id,
                principal='original-qq-principal', conversation='original-qq-conversation',
                workspace_id='removed-project', account_id='deleted-account', account_revision=1,
                transport_snapshot=json.dumps({'smtp_host': 'old.example.org', 'smtp_port': 465,
                    'smtp_security': 'ssl', 'username': 'owner@example.org', 'sender_address': 'owner@example.org'}),
                credential_ref='PRIVATE_CREDENTIAL_REFERENCE', scope_id='task-' + identifier,
                idempotency_key='key-' + identifier, request_hash='request-digest', content_hash='frozen-digest',
                content_json=json.dumps(content), wire=b'PRIVATE_SMTP_WIRE',
                message_id='<' + identifier + '@example.org>', state=state,
                result_json=json.dumps({'error': 'smtp_outcome_unknown', 'delivery_status': 'unconfirmed'}),
                created_at=clock.now().isoformat(), updated_at=clock.now().isoformat()))
        return identifier

    seed()
    seed('foreign-mail', owner_id='another-owner')
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
            yield SimpleNamespace(core=core, client=client, seed=seed,
                auth={'Authorization': 'Bearer delivery-admin-token'}, content=content)
    finally:
        await core.stop()
        core.db.close()


async def test_outgoing_metadata_and_details_require_admin_and_exclude_frozen_wire(delivery_admin):
    r = delivery_admin
    base = '/api/v1/mail/outgoing'
    for path in (base, base + '/unknown-mail'):
        assert (await r.client.get(path)).status_code == 401
        assert (await r.client.get(path, headers={'Authorization': 'Bearer wrong'})).status_code == 401
    response = await r.client.get(base, headers=r.auth)
    assert response.status_code == 200, response.text
    records = response.json()['outgoing']
    assert [row['id'] for row in records] == ['unknown-mail']
    assert records[0]['state'] == 'UNKNOWN' and records[0]['delivery_status'] == 'unconfirmed'
    assert records[0]['workspace_id'] == 'removed-project'
    assert 'PRIVATE_MESSAGE_BODY' not in response.text
    assert 'PRIVATE_CREDENTIAL_REFERENCE' not in response.text and 'PRIVATE_SMTP_WIRE' not in response.text
    detail = await r.client.get(base + '/unknown-mail', headers=r.auth)
    assert detail.status_code == 200, detail.text
    assert detail.json()['content']['body'] == 'PRIVATE_MESSAGE_BODY'
    assert detail.json()['content']['bcc'] == ['hidden@example.org']
    assert detail.json()['account_revision'] == 1
    assert detail.json()['transport_snapshot']['smtp_host'] == 'old.example.org'
    assert 'PRIVATE_CREDENTIAL_REFERENCE' not in detail.text and 'PRIVATE_SMTP_WIRE' not in detail.text
    assert detail.headers['cache-control'] == 'no-store'
    assert (await r.client.get(base + '/foreign-mail', headers=r.auth)).status_code == 404


@pytest.mark.parametrize('state,expected_smtp', [
    ('accepted', 'ACCEPTED'), ('delivered', 'ACCEPTED'), ('bounced', 'ACCEPTED'), ('not_sent', 'FAILED')])
async def test_operator_evidence_reconciles_history_without_sending(delivery_admin, state, expected_smtp):
    r = delivery_admin
    base = '/api/v1/mail/outgoing/unknown-mail'

    def no_sending(*args, **kwargs):
        raise AssertionError('receipt reconciliation must not contact SMTP')

    r.core.mail.sender.transport.send = no_sending
    evidence = 'Operator verified <unknown-mail@example.org> using provider logs and the recipient mailbox.'
    request = {'state': state, 'evidence': evidence}
    assert (await r.client.post(base + '/resolve', json=request)).status_code == 401
    response = await r.client.post(base + '/resolve', headers=r.auth, json=request)
    assert response.status_code == 200, response.text
    receipt = response.json()
    assert receipt['state'] == expected_smtp and receipt['delivery_status'] == state
    assert receipt['digest'] == 'frozen-digest'
    assert receipt['result']['reconciliation_evidence'] == evidence
    with r.core.db.engine.connect() as conn:
        row = conn.execute(select(mail_outgoing).where(mail_outgoing.c.id == 'unknown-mail')).mappings().one()
    assert row['wire'] == b'PRIVATE_SMTP_WIRE' and json.loads(row['content_json']) == r.content
    assert (await r.client.post(base + '/send', headers=r.auth, json={})).status_code in {404, 405}
    assert (await r.client.post('/api/v1/mail/outgoing/foreign-mail/resolve',
        headers=r.auth, json=request)).status_code == 404


async def test_resolution_validation_and_admin_policy_are_enforced(delivery_admin):
    r = delivery_admin
    base = '/api/v1/mail/outgoing'
    for body in ({'state': 'delivered', 'evidence': ''}, {'state': 'delivered', 'evidence': '   '},
                 {'state': 'resend', 'evidence': 'retry'}, {'state': 'delivered', 'evidence': 'x' * 4001}):
        assert (await r.client.post(base + '/unknown-mail/resolve', headers=r.auth, json=body)).status_code == 422
    r.seed('unsent-draft', state='DRAFT')
    assert (await r.client.post(base + '/unsent-draft/resolve', headers=r.auth,
        json={'state': 'delivered', 'evidence': 'unsupported state change'})).status_code == 422
    r.core.permissions.set_policy(PermissionPolicy(rules=(PermissionRule(id='deny-outgoing-admin',
        effect='deny', actions=('admin.*',), resources=('http:/api/v1/mail/outgoing*',)),)),
        expected_version=0, actor='delivery-test')
    assert (await r.client.get(base, headers=r.auth)).status_code == 403
    assert (await r.client.get(base + '/unknown-mail', headers=r.auth)).status_code == 403
    assert (await r.client.post(base + '/unknown-mail/resolve', headers=r.auth,
        json={'state': 'delivered', 'evidence': 'blocked operator'})).status_code == 403


async def test_outgoing_pagination_filter_and_public_ui_shell(delivery_admin):
    r = delivery_admin
    r.seed('accepted-mail', state='ACCEPTED')
    base = '/api/v1/mail/outgoing'
    first = await r.client.get(base + '?limit=1', headers=r.auth)
    second = await r.client.get(base + '?limit=1&offset=1', headers=r.auth)
    assert first.status_code == second.status_code == 200
    assert first.json()['next_offset'] == 1
    assert first.json()['outgoing'][0]['id'] != second.json()['outgoing'][0]['id']
    filtered = await r.client.get(base + '?state=UNKNOWN', headers=r.auth)
    assert [row['id'] for row in filtered.json()['outgoing']] == ['unknown-mail']
    assert (await r.client.get(base + '?limit=0', headers=r.auth)).status_code == 422
    page = await r.client.get('/api/v1/mail/outgoing-ui')
    assert page.status_code == 200
    assert 'type="password"' in page.text and 'textContent' in page.text
    assert all(value not in page.text for value in ('PRIVATE_MESSAGE_BODY', 'PRIVATE_CREDENTIAL_REFERENCE',
        'PRIVATE_SMTP_WIRE', 'delivery-admin-token'))
