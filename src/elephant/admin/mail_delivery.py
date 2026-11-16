"""Read protected outgoing receipts and reconcile them with operator evidence."""
import json
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select

from elephant.domain import ConversationRef, InboundEvent, PrincipalRef, Surface, TextSegment
from elephant.permissions import PermissionSubject
from elephant.storage.mail_send_schema import mail_outgoing

DELIVERY_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>邮件发送记录</title>
  <style>
    body {
      font: 16px system-ui, sans-serif;
      max-width: 960px;
      margin: 40px auto;
      padding: 0 20px;
      background: #f7f8fa;
      color: #18202a;
    }
    section, form {
      background: white;
      border: 1px solid #dbe0e6;
      border-radius: 12px;
      padding: 20px;
      margin: 20px 0;
    }
    label {
      display: block;
      margin: 12px 0;
    }
    input, select, textarea, button {
      font: inherit;
      padding: 8px;
      box-sizing: border-box;
    }
    input, textarea {
      display: block;
      width: 100%;
      margin-top: 6px;
    }
    button {
      cursor: pointer;
      margin: 4px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      text-align: left;
      border-bottom: 1px solid #dbe0e6;
      padding: 8px;
      overflow-wrap: anywhere;
    }
    pre {
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }
    textarea {
      min-height: 90px;
    }
  </style>
</head>
<body>
  <h1>邮件发送记录</h1>
  <p>SMTP 已接受表示服务器收下了邮件，最终投递需核对证据。UNKNOWN 表示发送结果待核对。</p>
  <label>
    管理员令牌
    <input id="token" type="password" autocomplete="off">
  </label>
  <label>
    发送状态
    <select id="filter">
      <option value="">全部</option>
      <option>UNKNOWN</option>
      <option>ACCEPTED</option>
      <option>PARTIAL_ACCEPTED</option>
      <option>FAILED</option>
      <option>DRAFT</option>
      <option>DISPATCHING</option>
    </select>
  </label>
  <button id="load">加载记录</button>
  <button id="previous" disabled>上一页</button>
  <button id="next" disabled>下一页</button>
  <section>
    <table>
      <thead>
        <tr><th>时间</th><th>主题</th><th>SMTP 状态</th><th>投递状态</th></tr>
      </thead>
      <tbody id="records"></tbody>
    </table>
  </section>
  <section>
    <h2>选中邮件</h2>
    <pre id="detail">选择记录查看已冻结的收件人、正文、附件摘要及 Message-ID。</pre>
  </section>
  <form id="resolve">
    <h2>记录人工核对结果</h2>
    <label>
      核对状态
      <select id="resolution">
        <option value="accepted">服务器已接受</option>
        <option value="delivered">已确认投递</option>
        <option value="bounced">已确认退信</option>
        <option value="not_sent">已确认未发送</option>
      </select>
    </label>
    <label>
      核对证据
      <textarea id="evidence" required maxlength="4000" placeholder="说明核对来源及对应 Message-ID"></textarea>
    </label>
    <button id="save" disabled>保存核对结果</button>
  </form>
  <p id="status" role="status" aria-live="polite">管理员令牌仅保存在当前页面内存中。</p>
  <script>
    const base = '/api/v1/mail/outgoing';
    const byId = id => document.getElementById(id);
    let offset = 0;
    let nextOffset = null;
    let selected = null;

    async function request(path, method = 'GET', body) {
      const response = await fetch(path, {
        method,
        headers: {
          Authorization: 'Bearer ' + byId('token').value,
          'Content-Type': 'application/json'
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        cache: 'no-store'
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.detail || '请求失败');
      }
      return result;
    }

    async function action(work) {
      try {
        await work();
      } catch (error) {
        byId('status').textContent = error.message;
      }
    }

    async function detail(id) {
      selected = await request(base + '/' + encodeURIComponent(id));
      byId('detail').textContent = JSON.stringify(selected, null, 2);
      byId('save').disabled = !['UNKNOWN', 'ACCEPTED', 'PARTIAL_ACCEPTED'].includes(selected.state);
    }

    async function load() {
      const params = new URLSearchParams({offset: String(offset)});
      if (byId('filter').value) {
        params.set('state', byId('filter').value);
      }
      const result = await request(base + '?' + params);
      nextOffset = result.next_offset;
      byId('records').replaceChildren();
      for (const record of result.outgoing) {
        const row = document.createElement('tr');
        const values = [record.created_at, record.subject || '（无主题）', record.state, record.delivery_status];
        for (const [index, value] of values.entries()) {
          const cell = document.createElement('td');
          if (index === 1) {
            const button = document.createElement('button');
            button.type = 'button';
            button.textContent = value;
            button.onclick = () => action(() => detail(record.id));
            cell.append(button);
          } else {
            cell.textContent = value;
          }
          row.append(cell);
        }
        byId('records').append(row);
      }
      byId('previous').disabled = offset === 0;
      byId('next').disabled = nextOffset === null;
      byId('status').textContent = '已加载 ' + result.outgoing.length + ' 条记录';
    }

    byId('load').onclick = () => action(async () => {
      offset = 0;
      await load();
    });
    byId('filter').onchange = () => {
      offset = 0;
    };
    byId('previous').onclick = () => action(async () => {
      offset = Math.max(0, offset - 50);
      await load();
    });
    byId('next').onclick = () => action(async () => {
      if (nextOffset !== null) {
        offset = nextOffset;
        await load();
      }
    });
    byId('resolve').onsubmit = event => {
      event.preventDefault();
      action(async () => {
        if (!selected) {
          throw new Error('请先选择邮件');
        }
        selected = await request(base + '/' + encodeURIComponent(selected.id) + '/resolve', 'POST', {
          state: byId('resolution').value,
          evidence: byId('evidence').value
        });
        byId('evidence').value = '';
        await load();
        await detail(selected.id);
        byId('status').textContent = '核对结果已保存';
      });
    };
  </script>
</body>
</html>"""


class DeliveryResolution(BaseModel):
    model_config = ConfigDict(extra='forbid')
    state: Literal['accepted', 'delivered', 'bounced', 'not_sent', 'failed']
    evidence: str = Field(min_length=1, max_length=4000)


def build_mail_delivery_router(core, admin):
    router = APIRouter(prefix='/api/v1/mail')

    @router.get('/outgoing-ui')
    def delivery_page():
        return HTMLResponse(DELIVERY_PAGE, headers={'Cache-Control': 'no-store'})

    def authorize(request: Request):
        subject = PermissionSubject(adapter_instance_id='admin_api', identity_namespace='admin_api', user_id='owner')
        core.permissions.require(subject, 'admin.' + request.method.lower(), 'http:' + request.url.path)

    protected = APIRouter(prefix='/outgoing', dependencies=[Depends(admin), Depends(authorize)])

    def event():
        now = core.clock.now()
        return InboundEvent(event_id=str(uuid4()), event_type='message.created',
            principal=PrincipalRef(adapter_instance_id='admin_api', identity_namespace='admin_api',
                remote_user_id=core.settings.owner.id),
            conversation=ConversationRef(deployment_id=core.settings.deployment_id, adapter_instance_id='admin_api',
                bot_identity_id='host', surface=Surface.PRIVATE, remote_conversation_id=core.settings.owner.id),
            platform_message_id='admin-mail-delivery', received_at=now, occurred_at=now,
            segments=(TextSegment(text='核对邮件发送记录'),), trace_id=str(uuid4()))

    def owned_workspace(identifier):
        with core.db.engine.connect() as conn:
            workspace = conn.execute(select(mail_outgoing.c.workspace_id).where(mail_outgoing.c.id == identifier,
                mail_outgoing.c.owner_id == core.settings.owner.id)).scalar_one_or_none()
        if workspace is None:
            raise LookupError('outgoing mail unavailable')
        return workspace

    @protected.get('')
    def list_outgoing(offset: int = Query(default=0, ge=0), limit: int = Query(default=50, ge=1, le=100),
                      state: Literal['DRAFT', 'DISPATCHING', 'UNKNOWN', 'ACCEPTED', 'PARTIAL_ACCEPTED', 'FAILED'] | None = None):
        source = event()
        if core.sessions.owner(source) != core.settings.owner.id or not core.allowed(source):
            raise PermissionError('mail requires an authenticated active owner')
        query = select(mail_outgoing.c.id, mail_outgoing.c.account_id, mail_outgoing.c.workspace_id,
            mail_outgoing.c.state, mail_outgoing.c.message_id, mail_outgoing.c.created_at, mail_outgoing.c.updated_at,
            func.json_extract(mail_outgoing.c.content_json, '$.subject').label('subject'),
            func.json_extract(mail_outgoing.c.content_json, '$.to').label('to_json'),
            func.json_extract(mail_outgoing.c.content_json, '$.cc').label('cc_json'),
            func.json_extract(mail_outgoing.c.content_json, '$.bcc').label('bcc_json'),
            func.json_extract(mail_outgoing.c.result_json, '$.delivery_status').label('delivery_status'))
        query = query.where(mail_outgoing.c.owner_id == core.settings.owner.id)
        if state:
            query = query.where(mail_outgoing.c.state == state)
        with core.db.engine.connect() as conn:
            rows = conn.execute(query.order_by(mail_outgoing.c.created_at.desc(), mail_outgoing.c.id.desc())
                .offset(offset).limit(limit)).mappings().all()
        outgoing = []
        for row in rows:
            if core.permissions.check(core.access(source), 'mail.status', 'mail-account:' + row['account_id']).allowed:
                outgoing.append({key: row[key] for key in ('id', 'account_id', 'workspace_id', 'state', 'message_id',
                    'created_at', 'updated_at', 'subject')} | {
                    name: json.loads(row[name + '_json'] or '[]') for name in ('to', 'cc', 'bcc')} | {
                    'delivery_status': row['delivery_status'] or 'unconfirmed'})
        return {'outgoing': outgoing, 'next_offset': offset + limit if len(rows) == limit else None}

    @protected.get('/{identifier}')
    def get_outgoing(identifier: str):
        return core.mail.sender.get(event(), owned_workspace(identifier), identifier, administrative=True)

    @protected.post('/{identifier}/resolve')
    def resolve_outgoing(identifier: str, body: DeliveryResolution):
        return core.mail.sender.resolve_delivery(event(), owned_workspace(identifier), identifier,
            state=body.state, evidence=body.evidence)

    router.include_router(protected)
    return router
