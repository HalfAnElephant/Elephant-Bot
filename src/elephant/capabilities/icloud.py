"""Owner-bound CalDAV access. Account credentials stay in the trusted Worker."""
import hashlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote, unquote, urljoin, urlsplit
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import httpx
from icalendar import Alarm, Calendar, Event, vRecur

from .types import Action, Capability

DAV = "DAV:"
CALDAV = "urn:ietf:params:xml:ns:caldav"
MAX_RESPONSE = 4 * 1024 * 1024
FIELDS = ("summary", "start", "end", "description", "location", "recurrence", "reminder_minutes")


def safe_url(url):
    parsed = urlsplit(url)
    hostname = (parsed.hostname or "").lower()
    if (parsed.scheme != "https" or parsed.username or parsed.password or parsed.port not in {None, 443}
            or parsed.query or parsed.fragment or not any(
                hostname.endswith("." + domain) for domain in ("icloud.com", "icloud.com.cn"))
            or any(ord(char) < 32 for char in url) or "\\" in url):
        raise ValueError("CalDAV target must be an HTTPS iCloud host")
    return url


class ICloudCalendarService:
    def __init__(self, config, *, transport=None):
        self.config = config
        self.owner = config.get("owner", {})
        if not all(isinstance(self.owner.get(key), str) and self.owner[key] for key in (
            "adapter_instance_id", "identity_namespace", "remote_user_id"
        )):
            raise ValueError("iCloud owner requires complete platform identity")
        if not config.get("apple_id") or not config.get("app_password"):
            raise ValueError("iCloud account and app-specific password are required")
        self.timezone = ZoneInfo(config.get("timezone", "Asia/Shanghai"))
        self.server = safe_url(config.get("server_url", "https://caldav.icloud.com/"))
        self.transport = transport

    @classmethod
    def from_file(cls, path):
        return cls(json.loads(Path(path).read_text()))

    def authorize(self, context):
        principal = context.principal
        if hasattr(principal, "model_dump"):
            principal = principal.model_dump()
        if not isinstance(principal, dict) or any(
            principal.get(key) != self.owner[key] for key in (
                "adapter_instance_id", "identity_namespace", "remote_user_id"
            )
        ):
            raise PermissionError("iCloud calendar is restricted to its account owner")

    def classify(self, arguments, context):
        self.authorize(context)
        operation = arguments.get("operation")
        if operation not in {"calendars", "list", "get", "create", "update", "delete"}:
            raise ValueError("unsupported iCloud calendar operation")
        return Action("icloud.calendar." + operation, ("icloud:calendar",),
                      "write" if operation in {"create", "update", "delete"} else "read")

    def register(self, registry):
        properties = {
            "operation": {"type": "string", "enum": ["calendars", "list", "get", "create", "update", "delete"]},
            "calendar_id": {"type": "string", "description": "ID returned by calendars, or an exact calendar name"},
            "event_id": {"type": "string", "description": "Event filename returned by list/get"},
            "summary": {"type": "string"},
            "start": {"type": "string", "description": "ISO date for all-day events, or ISO datetime with offset"},
            "end": {"type": "string", "description": "Exclusive end, same kind as start"},
            "timezone": {"type": "string", "description": "IANA timezone, default Asia/Shanghai"},
            "description": {"type": "string"}, "location": {"type": "string"},
            "recurrence": {"type": ["string", "null"], "description": "RRULE without RRULE: prefix, null removes"},
            "reminder_minutes": {"type": ["integer", "null"], "minimum": 0, "maximum": 525600},
        }
        registry.register(Capability("icloud.calendar",
            "iCloud 日历 日程 calendar events 查询 添加 创建 修改 删除。仅账号主人可用。"
            "Use calendars to discover calendars, list with bounded start/end, get before editing. "
            "create requires summary/start/end. update changes only supplied fields. "
            "All-day end is exclusive. Recurrence edits affect the entire series, not individual occurrences. "
            "reminder_minutes sets a display alarm, not an Apple Reminders task. "
            "External writes require user intent and readback verification.",
            {"type": "object", "properties": properties, "required": ["operation"],
             "additionalProperties": False}, timeout_seconds=120, max_output_bytes=32768),
            self.execute, self.classify)

    async def request(self, method, url, *, headers=None, content=None):
        safe_url(url)
        try:
            async with httpx.AsyncClient(auth=(self.config["apple_id"], self.config["app_password"]),
                                         transport=self.transport, timeout=20, follow_redirects=False,
                                         trust_env=False) as client:
                for _ in range(5):
                    async with client.stream(method, url, headers=headers, content=content) as response:
                        if response.status_code in {301, 302, 307, 308}:
                            location = response.headers.get("location")
                            if not location:
                                raise ValueError("CalDAV redirect has no destination")
                            url = safe_url(urljoin(url, location))
                            continue
                        data = bytearray()
                        async for chunk in response.aiter_bytes():
                            data.extend(chunk)
                            if len(data) > MAX_RESPONSE:
                                raise ValueError("CalDAV response exceeds size limit")
                        return response.status_code, response.headers, bytes(data), str(response.url)
            raise ValueError("CalDAV redirect limit exceeded")
        except httpx.HTTPError:
            raise RuntimeError("iCloud CalDAV connection failed; read back before retrying a write") from None

    async def xml(self, method, url, body, depth="0"):
        status, _, data, actual_url = await self.request(method, url, headers={
            "Depth": depth, "Content-Type": "application/xml; charset=utf-8"}, content=body.encode())
        if status != 207:
            raise RuntimeError(f"iCloud CalDAV discovery/query failed (HTTP {status})")
        if b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
            raise ValueError("CalDAV XML document declarations are forbidden")
        try:
            root = ET.fromstring(data)
        except ET.ParseError:
            raise ValueError("iCloud returned invalid CalDAV XML") from None
        return root, actual_url

    async def calendars(self):
        principal, base = await self.xml("PROPFIND", self.server,
            '<d:propfind xmlns:d="DAV:"><d:prop><d:current-user-principal/></d:prop></d:propfind>')
        href = principal.findtext(f".//{{{DAV}}}current-user-principal/{{{DAV}}}href")
        if not href:
            raise ValueError("iCloud principal discovery returned no principal")
        home, base = await self.xml("PROPFIND", safe_url(urljoin(base, href)),
            '<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">'
            '<d:prop><c:calendar-home-set/></d:prop></d:propfind>')
        href = home.findtext(f".//{{{CALDAV}}}calendar-home-set/{{{DAV}}}href")
        if not href:
            raise ValueError("iCloud calendar home discovery returned no home")
        listing, base = await self.xml("PROPFIND", safe_url(urljoin(base, href)),
            '<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">'
            '<d:prop><d:displayname/><d:resourcetype/><c:supported-calendar-component-set/>'
            '</d:prop></d:propfind>', "1")
        calendars = []
        for response in listing.findall(f"{{{DAV}}}response"):
            for propstat in response.findall(f"{{{DAV}}}propstat"):
                if " 200 " not in propstat.findtext(f"{{{DAV}}}status", ""):
                    continue
                prop = propstat.find(f"{{{DAV}}}prop")
                if prop is None or prop.find(f"{{{DAV}}}resourcetype/{{{CALDAV}}}calendar") is None:
                    continue
                supported = prop.findall(f"{{{CALDAV}}}supported-calendar-component-set/{{{CALDAV}}}comp")
                if supported and not any(component.get("name") == "VEVENT" for component in supported):
                    continue
                href = response.findtext(f"{{{DAV}}}href")
                if not href:
                    continue
                url = safe_url(urljoin(base, href)).rstrip("/") + "/"
                calendars.append({"calendar_id": urlsplit(url).path,
                                  "name": prop.findtext(f"{{{DAV}}}displayname", ""), "url": url})
        return calendars

    async def calendar(self, arguments):
        calendars = await self.calendars()
        selection = arguments.get("calendar_id") or self.config.get("default_calendar")
        matches = [item for item in calendars if selection in {item["calendar_id"], item["name"]}]
        if selection and len(matches) != 1:
            raise ValueError("calendar_id must identify exactly one discovered iCloud calendar")
        if not selection:
            if len(calendars) != 1:
                raise ValueError("choose calendar_id from calendars before accessing events")
            return calendars[0]
        return matches[0]

    def event_url(self, calendar, event_id):
        if (not isinstance(event_id, str) or not re.fullmatch(r"[\w.@-]{1,200}\.ics", event_id)
                or event_id.startswith(".")):
            raise ValueError("event_id must be a discovered .ics filename")
        return safe_url(calendar["url"] + quote(event_id, safe=""))

    def parse(self, content):
        try:
            calendar = Calendar.from_ical(content)
        except (ValueError, TypeError, KeyError):
            raise ValueError("iCloud returned invalid iCalendar content") from None
        events = calendar.walk("VEVENT")
        if not events or any(not event.get("UID") for event in events) or len({str(event["UID"]) for event in events}) != 1:
            raise ValueError("iCloud resource must contain events sharing one UID")
        return calendar, events

    def time(self, value, timezone):
        if not isinstance(value, str):
            raise ValueError("event dates must be ISO date or timezone-aware datetime strings")  # noqa: TRY004 - tool protocol error
        try:
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
                return date.fromisoformat(value)
            result = datetime.fromisoformat(value)
            if result.tzinfo is None:
                raise ValueError("event datetime requires a timezone offset")
            return result.astimezone(timezone)
        except ValueError:
            raise ValueError("use ISO dates or ISO datetimes with explicit timezone offsets") from None

    def event_data(self, event):
        def decoded(name):
            value = event.get(name)
            if value is None:
                return None
            value = getattr(value, "dt", value)
            if isinstance(value, datetime) and value.tzinfo is None:
                value = value.replace(tzinfo=self.timezone)
            return value.isoformat() if isinstance(value, (date, datetime)) else str(value)
        alarms = []
        for alarm in event.walk("VALARM"):
            trigger = alarm.get("TRIGGER")
            value = getattr(trigger, "dt", None)
            if isinstance(value, timedelta):
                alarms.append(-value.total_seconds() / 60)
        recurrence = event.get("RRULE")
        end = decoded("DTEND")
        if end is None and event.get("DURATION") is not None and event.get("DTSTART") is not None:
            end = (event.decoded("DTSTART") + event.decoded("DURATION")).isoformat()
        return {"uid": decoded("UID"), "summary": decoded("SUMMARY"), "start": decoded("DTSTART"),
                "end": end, "description": decoded("DESCRIPTION"), "location": decoded("LOCATION"),
                "recurrence": recurrence.to_ical().decode() if recurrence else None,
                "reminder_minutes": alarms, "recurrence_id": decoded("RECURRENCE-ID")}

    async def read(self, calendar, event_id):
        status, headers, content, _ = await self.request("GET", self.event_url(calendar, event_id))
        if status == 404:
            return None
        if status != 200:
            raise RuntimeError(f"iCloud event read failed (HTTP {status})")
        document, events = self.parse(content)
        return {"etag": headers.get("etag"), "ics": content.decode(), "document": document, "events": events}

    def modified(self, document, event, arguments):
        timezone = ZoneInfo(arguments.get("timezone", str(self.timezone)))
        for name in ("summary", "description", "location"):
            if name in arguments:
                value = arguments[name]
                if (not isinstance(value, str) or len(value) > 16000 or any(
                    ord(char) < 32 and char not in "\n\t" for char in value
                ) or (name == "summary" and not value.strip())):
                    raise ValueError("event text must be bounded text without control characters")
                event.pop(name, None)
                event.add(name, value)
        if "location" in arguments:
            event.pop("X-APPLE-STRUCTURED-LOCATION", None)
        if "summary" in arguments:
            for alarm in event.walk("VALARM"):
                if str(alarm.get("ACTION", "")).upper() == "DISPLAY":
                    alarm.pop("DESCRIPTION", None)
                    alarm.add("DESCRIPTION", str(event["SUMMARY"]))
        for name, property_name in (("start", "DTSTART"), ("end", "DTEND")):
            if name in arguments:
                event.pop(property_name, None)
                event.add(property_name, self.time(arguments[name], timezone))
        start, end = event.decoded("DTSTART", None), event.decoded("DTEND", None)
        if end is None and start is not None and event.get("DURATION") is not None:
            end = start + event.decoded("DURATION")
        if not start or not end or type(start) is not type(end) or end <= start:
            raise ValueError("start/end must have the same kind and end must follow start; all-day end is exclusive")
        if "end" in arguments:
            event.pop("DURATION", None)
        if "recurrence" in arguments:
            event.pop("RRULE", None)
            recurrence = arguments["recurrence"]
            if recurrence is not None:
                if not isinstance(recurrence, str) or len(recurrence) > 1000 or "\n" in recurrence or "\r" in recurrence:
                    raise ValueError("recurrence must be a bounded RRULE value")
                try:
                    rule = vRecur.from_ical(recurrence)
                    if "FREQ" not in rule:
                        raise ValueError("RRULE requires FREQ")
                    event.add("RRULE", rule)
                except (ValueError, TypeError, KeyError):
                    raise ValueError("invalid recurrence RRULE") from None
        if "reminder_minutes" in arguments:
            minutes = arguments["reminder_minutes"]
            if minutes is not None and (type(minutes) is not int or not 0 <= minutes <= 525600):
                raise ValueError("reminder_minutes must be null or an integer from 0 to 525600")
            event.subcomponents = [component for component in event.subcomponents if component.name != "VALARM"]
            if minutes is not None:
                alarm = Alarm()
                alarm.add("ACTION", "DISPLAY")
                alarm.add("DESCRIPTION", str(event.get("SUMMARY", "日程提醒")))
                alarm.add("TRIGGER", -timedelta(minutes=minutes))
                event.add_component(alarm)
        event.pop("DTSTAMP", None)
        now = datetime.now(UTC)
        event.add("DTSTAMP", now)
        event.pop("LAST-MODIFIED", None)
        event.add("LAST-MODIFIED", now)
        if arguments["operation"] == "update":
            sequence = int(event.pop("SEQUENCE", 0))
            event.add("SEQUENCE", sequence + 1)
        document.add_missing_timezones()
        return document.to_ical().decode()

    async def prepare(self, context, action, arguments):
        self.classify(arguments, context)
        calendar = await self.calendar(arguments)
        operation = arguments["operation"]
        if operation == "create":
            if not all(key in arguments for key in ("summary", "start", "end")):
                raise ValueError("creating an event requires summary, start and end")
            supplied = {key: value for key, value in arguments.items() if not key.startswith("_")}
            digest = hashlib.sha256((context.task_id + json.dumps(supplied, sort_keys=True,
                                     ensure_ascii=False)).encode()).hexdigest()
            event_id, uid = "elephant-" + digest + ".ics", "elephant-" + digest
            document, event = Calendar(), Event()
            document.add("VERSION", "2.0")
            document.add("PRODID", "-//Elephant-Bot//iCloud Calendar//EN")
            event.add("UID", uid)
            document.add_component(event)
            expected = self.modified(document, event, arguments)
            return {"calendar_id": calendar["calendar_id"], "event_id": event_id, "expected_ics": expected}
        event_id = arguments.get("event_id")
        existing = await self.read(calendar, event_id)
        if not existing:
            if operation == "delete":
                return {"calendar_id": calendar["calendar_id"], "event_id": event_id, "already_absent": True}
            raise ValueError("event no longer exists")
        if not existing["etag"]:
            raise ValueError("iCloud event has no ETag; refusing an unconditional change")
        preconditions = {"calendar_id": calendar["calendar_id"], "event_id": event_id,
                         "etag": existing["etag"], "before_ics": existing["ics"]}
        if operation == "update":
            masters = [event for event in existing["events"] if "RECURRENCE-ID" not in event]
            if len(masters) != 1 or len(existing["events"]) != 1:
                raise ValueError("editing a series with detached occurrences requires a calendar client")
            if not any(key in arguments for key in FIELDS):
                raise ValueError("update requires at least one event field")
            preconditions["expected_ics"] = self.modified(existing["document"], masters[0], arguments)
        return preconditions

    async def execute(self, arguments, context):
        self.classify(arguments, context)
        operation = arguments["operation"]
        if operation == "calendars":
            return [{key: value for key, value in item.items() if key != "url"} for item in await self.calendars()]
        calendar = await self.calendar(arguments)
        if operation == "list":
            timezone = ZoneInfo(arguments.get("timezone", str(self.timezone)))
            start, end = self.time(arguments.get("start"), timezone), self.time(arguments.get("end"), timezone)
            if type(start) is not type(end) or end <= start or end - start > timedelta(days=366):
                raise ValueError("list requires a positive start/end interval of at most 366 days")
            def utc(value):
                if not isinstance(value, datetime):
                    value = datetime.combine(value, datetime.min.time(), tzinfo=timezone)
                return value.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
            body = ('<c:calendar-query xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">'
                    '<d:prop><d:getetag/><c:calendar-data/></d:prop><c:filter><c:comp-filter name="VCALENDAR">'
                    '<c:comp-filter name="VEVENT"><c:time-range start="' + utc(start) + '" end="' + utc(end) + '"/>'
                    '</c:comp-filter></c:comp-filter></c:filter></c:calendar-query>')
            root, _ = await self.xml("REPORT", calendar["url"], body, "1")
            result = []
            for response in root.findall(f"{{{DAV}}}response"):
                content = response.findtext(f".//{{{CALDAV}}}calendar-data")
                if not content:
                    continue
                href = response.findtext(f"{{{DAV}}}href", "")
                url = safe_url(urljoin(calendar["url"], href))
                if not url.startswith(calendar["url"]):
                    raise ValueError("CalDAV event lies outside the selected calendar")
                event_id = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
                self.event_url(calendar, event_id)
                _, events = self.parse(content)
                result.append({"calendar_id": calendar["calendar_id"], "event_id": event_id,
                               "events": [self.event_data(event) for event in events]})
            return {"events": result, "recurrences_expanded": False}
        if operation == "get":
            event_id = arguments.get("event_id")
            existing = await self.read(calendar, event_id)
            return {"calendar_id": calendar["calendar_id"], "event_id": event_id, "exists": bool(existing),
                    "events": [self.event_data(event) for event in existing["events"]] if existing else []}
        pre = arguments.get("_preconditions")
        if not pre:
            raise ValueError("calendar write requires durable preconditions")
        if operation == "delete" and pre.get("already_absent"):
            return {"http_status": 404, "calendar_id": pre["calendar_id"], "event_id": pre["event_id"]}
        calendar = await self.calendar({"calendar_id": pre["calendar_id"]})
        url = self.event_url(calendar, pre["event_id"])
        headers = {"If-None-Match": "*"} if operation == "create" else {"If-Match": pre["etag"]}
        headers["Content-Type"] = "text/calendar; charset=utf-8"
        status, _, _, _ = await self.request("DELETE" if operation == "delete" else "PUT", url,
            headers=headers, content=None if operation == "delete" else pre["expected_ics"].encode())
        return {"http_status": status, "calendar_id": pre["calendar_id"], "event_id": pre["event_id"]}

    async def verify(self, context, action, arguments, data):
        self.classify(arguments, context)
        pre = arguments.get("_preconditions", {})
        if not pre:
            return "UNKNOWN", {"error": "calendar write has no durable preconditions"}
        calendar = await self.calendar({"calendar_id": pre["calendar_id"]})
        existing = await self.read(calendar, pre["event_id"])
        result = {"calendar_id": pre["calendar_id"], "event_id": pre["event_id"],
                  "exists": bool(existing)}
        status = data.get("http_status") if isinstance(data, dict) else None
        if status == 412 and arguments["operation"] != "create":
            return "FAILED", {**result, "http_status": status,
                              "error": "event changed after it was read; conditional write was rejected"}
        if arguments["operation"] == "delete":
            if not existing:
                return "VERIFIED", result
        elif existing:
            _, expected = self.parse(pre["expected_ics"])
            wanted = self.event_data(expected[0])
            observed = [self.event_data(event) for event in existing["events"]]
            # Servers may normalize timezone representations and reorder RRULE members.
            def comparable(item):
                item = dict(item)
                for key in ("start", "end"):
                    if item[key] and "T" in item[key]:
                        item[key] = datetime.fromisoformat(item[key]).astimezone(UTC).isoformat()
                if item["recurrence"]:
                    item["recurrence"] = vRecur.from_ical(item["recurrence"]).to_ical().decode()
                return item
            if len(observed) == 1 and comparable(observed[0]) == comparable(wanted):
                return "VERIFIED", {**result, "events": observed}
        if status and 400 <= status < 500:
            return "FAILED", {**result, "http_status": status,
                              "error": "iCloud rejected the conditional write; event may have changed"}
        return "UNKNOWN", {**result, "error": "readback does not confirm the requested calendar effect"}
