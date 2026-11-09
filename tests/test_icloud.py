"""CalDAV protocol checks use a local transport and never contact an Apple account."""
import json
from dataclasses import replace
from datetime import UTC, timedelta
from xml.sax.saxutils import escape

import httpx
import pytest
from icalendar import Calendar

from qqbot.capabilities import CapabilityRegistry, SQLiteActionLedger, ToolContext, ToolGateway
from qqbot.capabilities.icloud import ICloudCalendarService, safe_url
from qqbot.harness.gateway import RunGateway
from qqbot.storage.action_schema import operations
from qqbot.storage.db import Database

OWNER = {"adapter_instance_id": "fixture_adapter", "identity_namespace": "qq", "remote_user_id": "owner-user"}
CALENDAR_ID = "/123/calendars/work/"
EVENT = {"operation": "create", "summary": "讨论", "start": "2026-11-10T09:00:00+08:00",
         "end": "2026-11-10T10:00:00+08:00", "calendar_id": CALENDAR_ID}


class CalDAVFixture:
    def __init__(self):
        self.resources, self.calls = {}, []
        self.revision = 0
        self.redirect = None
        self.conflict = False
        self.lose_write_response = False

    def multistatus(self, body):
        return httpx.Response(207, content=('<d:multistatus xmlns:d="DAV:" '
            'xmlns:c="urn:ietf:params:xml:ns:caldav">' + body + '</d:multistatus>').encode())

    def __call__(self, request):
        self.calls.append(request)
        path = request.url.path
        if request.method == "PROPFIND":
            if path == "/":
                if self.redirect:
                    return httpx.Response(301, headers={"Location": self.redirect})
                return self.multistatus('<d:response><d:propstat><d:prop><d:current-user-principal>'
                    '<d:href>/123/principal/</d:href></d:current-user-principal></d:prop>'
                    '<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>')
            if path == "/123/principal/":
                return self.multistatus('<d:response><d:propstat><d:prop><c:calendar-home-set>'
                    '<d:href>https://p01-caldav.icloud.com/123/calendars/</d:href></c:calendar-home-set>'
                    '</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>')
            assert path == "/123/calendars/"
            return self.multistatus('<d:response><d:href>' + CALENDAR_ID + '</d:href><d:propstat><d:prop>'
                '<d:displayname>工作</d:displayname><d:resourcetype><d:collection/><c:calendar/>'
                '</d:resourcetype><c:supported-calendar-component-set><c:comp name="VEVENT"/>'
                '</c:supported-calendar-component-set></d:prop><d:status>HTTP/1.1 200 OK</d:status>'
                '</d:propstat></d:response>'
                '<d:response><d:href>/123/calendars/reminders/</d:href><d:propstat><d:prop>'
                '<d:displayname>提醒事项</d:displayname><d:resourcetype><c:calendar/></d:resourcetype>'
                '<c:supported-calendar-component-set><c:comp name="VTODO"/></c:supported-calendar-component-set>'
                '</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>')
        if request.method == "REPORT":
            return self.multistatus('<d:response><d:href>' + CALENDAR_ID + '</d:href><d:propstat><d:prop/>'
                '<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>' + "".join(
                '<d:response><d:href>' + path + '</d:href><d:propstat><d:prop>'
                '<c:calendar-data>' + escape(content.decode()) + '</c:calendar-data></d:prop>'
                '<d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>'
                for path, (content, _) in self.resources.items()))
        if request.method == "GET":
            if path not in self.resources:
                return httpx.Response(404)
            content, etag = self.resources[path]
            return httpx.Response(200, content=content, headers={"ETag": etag})
        assert request.method in {"PUT", "DELETE"}
        if self.conflict:
            return httpx.Response(412)
        current = self.resources.get(path)
        if request.headers.get("if-none-match") == "*":
            if current:
                return httpx.Response(412)
        else:
            assert request.headers.get("if-match")
            if not current or request.headers["if-match"] != current[1]:
                return httpx.Response(412)
        if request.method == "DELETE":
            self.resources.pop(path, None)
        else:
            self.revision += 1
            self.resources[path] = (request.content, f'"{self.revision}"')
        if self.lose_write_response:
            self.lose_write_response = False
            raise httpx.ReadError("sensitive network error", request=request)
        return httpx.Response(204 if request.method == "DELETE" else 201)


@pytest.fixture
def calendar(tmp_path):
    remote = CalDAVFixture()
    service = ICloudCalendarService({"apple_id": "owner@example.invalid", "app_password": "fixture-password",
        "owner": OWNER, "timezone": "Asia/Shanghai", "default_calendar": "工作"},
        transport=httpx.MockTransport(remote))
    context = ToolContext("task", "run", 1, OWNER, str(tmp_path))
    database = Database(str(tmp_path / "calendar.db"))
    operations.create(database.engine)
    ledger = SQLiteActionLedger(database)
    registry = CapabilityRegistry()
    service.register(registry)
    gateway = ToolGateway(registry, authority=lambda _: True, permission=lambda *_: True,
                          ledger=ledger, verify=service.verify, prepare=service.prepare)
    yield service, remote, context, gateway, ledger
    database.close()


async def invoke(calendar, arguments, operation_id=None):
    return await calendar[3].invoke("icloud.calendar", arguments, calendar[2], operation_id=operation_id)


async def test_discovery_and_event_crud_preserve_unknown_fields(calendar):
    _, remote, _, _, _ = calendar
    listing = await invoke(calendar, {"operation": "calendars"})
    assert listing.data == [{"calendar_id": CALENDAR_ID, "name": "工作"}]
    created = await invoke(calendar, {**EVENT, "recurrence": "FREQ=WEEKLY;COUNT=4", "reminder_minutes": 15})
    assert created.status == "verified"
    event_id = created.data["event_id"]
    path = CALENDAR_ID + event_id
    document = Calendar.from_ical(remote.resources[path][0])
    event = document.walk("VEVENT")[0]
    assert event.decoded("DTSTART").hour == 9
    assert str(event.decoded("DTSTART").tzinfo) == "Asia/Shanghai"
    assert event.decoded("DTSTART").astimezone(UTC).hour == 1
    assert document.walk("VTIMEZONE")[0]["TZID"] == "Asia/Shanghai"
    assert event.walk("VALARM")[0].decoded("TRIGGER") == -timedelta(minutes=15)
    event.add("X-APPLE-CUSTOM", "keep this")
    event.add("X-APPLE-STRUCTURED-LOCATION", "geo:31.0,121.0")
    event.add("SEQUENCE", 7)
    remote.resources[path] = (document.to_ical(), remote.resources[path][1])
    read = await invoke(calendar, {"operation": "get", "event_id": event_id})
    assert read.data["events"][0]["summary"] == "讨论"
    listed = await invoke(calendar, {"operation": "list", "start": "2026-11-01", "end": "2026-12-01"})
    assert listed.data["events"][0]["event_id"] == event_id
    assert listed.data["recurrences_expanded"] is False
    renamed = await invoke(calendar, {"operation": "update", "event_id": event_id,
                                      "summary": "重新命名", "location": "新位置"})
    assert renamed.status == "verified"
    event = Calendar.from_ical(remote.resources[path][0]).walk("VEVENT")[0]
    assert int(event["SEQUENCE"]) == 8
    assert event.get("LAST-MODIFIED") is not None
    assert "X-APPLE-STRUCTURED-LOCATION" not in event
    assert str(event.walk("VALARM")[0]["DESCRIPTION"]) == "重新命名"
    updated = await invoke(calendar, {"operation": "update", "event_id": event_id, "summary": "修改后",
                                      "recurrence": None, "reminder_minutes": None})
    assert updated.status == "verified"
    event = Calendar.from_ical(remote.resources[path][0]).walk("VEVENT")[0]
    assert str(event["X-APPLE-CUSTOM"]) == "keep this"
    assert "RRULE" not in event and event.walk("VALARM") == []
    deleted = await invoke(calendar, {"operation": "delete", "event_id": event_id})
    assert deleted.status == "verified" and path not in remote.resources
    assert any(request.headers.get("if-none-match") == "*" for request in remote.calls)
    assert all(request.headers.get("if-match") for request in remote.calls if request.method == "DELETE")


@pytest.mark.parametrize("field", ["remote_user_id", "adapter_instance_id", "identity_namespace"])
async def test_foreign_owner_rejected_before_network_and_reconciliation(calendar, field):
    service, remote, context, gateway, _ = calendar
    stranger = replace(context, principal={**OWNER, field: "other"})
    with pytest.raises(PermissionError):
        await gateway.invoke("icloud.calendar", {"operation": "calendars"}, stranger)
    action = service.classify(EVENT, context)
    with pytest.raises(PermissionError):
        await service.prepare(stranger, action, EVENT)
    with pytest.raises(PermissionError):
        await service.verify(stranger, action, EVENT, {})
    assert remote.calls == []


def test_owner_config_requires_all_identity_fields():
    with pytest.raises(ValueError, match="complete platform identity"):
        ICloudCalendarService({"apple_id": "a", "app_password": "b", "owner": {"remote_user_id": "1"}})


async def test_semantic_duplicate_and_conditional_create_are_idempotent(calendar):
    service, remote, context, _, _ = calendar
    first = await invoke(calendar, EVENT, "first")
    second = await invoke(calendar, EVENT, "second")
    assert first.status == second.status == "verified"
    assert len([request for request in remote.calls if request.method == "PUT"]) == 1
    pre = await service.prepare(context, service.classify(EVENT, context), EVENT)
    arguments = {**EVENT, "_preconditions": pre}
    result = await service.execute(arguments, context)
    assert result["http_status"] == 412
    assert (await service.verify(context, service.classify(EVENT, context), arguments, result))[0] == "VERIFIED"
    assert len(remote.resources) == 1


async def test_lost_write_response_reconciles_without_another_write(calendar):
    service, remote, context, _, ledger = calendar
    remote.lose_write_response = True
    with pytest.raises(RuntimeError, match="read back") as error:
        await invoke(calendar, EVENT, "lost")
    assert "sensitive" not in str(error.value)
    with ledger.database.engine.connect() as connection:
        row = connection.execute(operations.select().where(operations.c.id == "lost")).mappings().one()
    assert row["state"] == "UNKNOWN"
    arguments = json.loads(row["arguments_json"])
    state, result = await service.verify(context, service.classify(EVENT, context), arguments, {})
    assert state == "VERIFIED" and result["exists"]
    assert len([request for request in remote.calls if request.method == "PUT"]) == 1


async def test_update_conflict_never_overwrites(calendar):
    service, remote, context, _, _ = calendar
    created = await invoke(calendar, EVENT)
    arguments = {"operation": "update", "event_id": created.data["event_id"], "summary": "new"}
    action = service.classify(arguments, context)
    pre = await service.prepare(context, action, arguments)
    remote.conflict = True
    arguments["_preconditions"] = pre
    result = await service.execute(arguments, context)
    assert (await service.verify(context, action, arguments, result))[0] == "FAILED"
    event = Calendar.from_ical(next(iter(remote.resources.values()))[0]).walk("VEVENT")[0]
    assert str(event["SUMMARY"]) == "讨论"


async def test_all_day_exclusive_end_and_invalid_dates(calendar):
    created = await invoke(calendar, {**EVENT, "start": "2026-11-10", "end": "2026-11-11"})
    assert created.status == "verified"
    assert created.data["events"][0]["start"] == "2026-11-10"
    assert created.data["events"][0]["end"] == "2026-11-11"
    for changes in ({"start": "2026-11-10T09:00:00"}, {"end": EVENT["start"]},
                    {"start": "2026-11-10"}, {"summary": "bad\r\nUID:injected"}):
        with pytest.raises(ValueError):
            await invoke(calendar, {**EVENT, **changes})


@pytest.mark.parametrize("url", ["http://caldav.icloud.com/", "https://icloud.com.evil.invalid/",
    "https://evil.invalid/", "https://caldav.icloud.com:444/", "https://user:pass@caldav.icloud.com/",
    "https://caldav.icloud.com/path?token=x", "https://caldav.icloud.com/path#x"])
def test_address_allowlist(url):
    with pytest.raises(ValueError):
        safe_url(url)


async def test_external_redirect_rejected_before_credentials_leave_icloud(calendar):
    service, remote, context, _, _ = calendar
    remote.redirect = "https://attacker.invalid/"
    with pytest.raises(ValueError, match="HTTPS iCloud"):
        await service.execute({"operation": "calendars"}, context)
    assert len(remote.calls) == 1


@pytest.mark.parametrize("event_id", ["../other.ics", "https://evil.invalid/x.ics", "x%2fother.ics", ".ics"])
async def test_event_path_cannot_escape_calendar(calendar, event_id):
    with pytest.raises(ValueError, match="filename"):
        await invoke(calendar, {"operation": "get", "event_id": event_id})


async def test_gateway_catalog_and_durable_write_routing(calendar, tmp_path):
    service, remote, _, _, ledger = calendar
    async def core(operation, payload):
        if operation == "authorize":
            return {"allowed": True}
        if operation == "reserve-budget":
            return {"accepted": True}
        if operation in {"observation", "usage"}:
            return {"recorded": True}
        if operation == "ledger":
            if payload["operation"] == "prepare":
                from qqbot.capabilities import Action
                return await ledger.prepare(payload["operation_id"], gateway.context,
                    Action(**payload["action"]), payload["arguments_hash"], payload["arguments"])
            if payload["operation"] == "transition":
                return {"changed": await ledger.transition(payload["operation_id"], payload["expected"],
                                                            payload["state"], payload["result"])}
        raise AssertionError(operation)
    gateway = RunGateway(tmp_path / "run", "fixture", "fixture-secret", "https://model.invalid/v1/chat/completions",
                         call=core, icloud=service)
    gateway.bind({"task_id": "task", "run_id": "run", "fencing_token": 1,
                  "permission_snapshot": {"principal": OWNER}, "context": {}, "goal": "添加日程"})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=gateway.app), base_url="http://gateway") as client:
        headers = {"Authorization": "Bearer " + gateway.tool_token}
        catalog = (await client.post("/tools/capability_search", headers=headers,
            json={"call_id": "search", "arguments": {"query": "添加日程"}})).json()
        assert catalog["data"][0]["name"] == "icloud.calendar"
        response = await client.post("/tools/capability_invoke", headers=headers,
            json={"call_id": "write", "arguments": {"name": "icloud.calendar", "arguments": EVENT}})
    assert response.status_code == 200
    assert response.json()["status"] == "verified"
    with ledger.database.engine.connect() as connection:
        row = connection.execute(operations.select()).mappings().one()
    preconditions = json.loads(row["arguments_json"])["_preconditions"]
    assert "expected_ics" in preconditions and "fixture-password" not in row["arguments_json"]
    assert row["state"] == "VERIFIED"
    assert any(request.method == "PUT" for request in remote.calls)
