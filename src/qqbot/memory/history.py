"""Core-owned history access with fresh scope, evidence and permission checks."""
import asyncio
from collections import OrderedDict

from sqlalchemy import select

from qqbot.domain import InboundEvent
from qqbot.memory import keywords
from qqbot.storage.schema import inbox
from qqbot.storage.social_schema import memory_source_retention, social_tombstones


class HistoryMemoryService:
    def __init__(self, social, embedding=None):
        self.social, self.db, self.permissions = social, social.db, social.permissions
        self.embedding = embedding
        self._queries = OrderedDict()

    async def query_options(self, access, query):
        provider = self.embedding
        if provider is None or not provider.api_key or len(query.strip()) < 4:
            return {}
        self.permissions.require(access, "memory.read", "conversation:" + access.conversation.key)
        self.permissions.require(access, "model.request", "conversation:" + access.conversation.key)
        key = (access.conversation.key, access.principal.key, self.social.version(access),
               provider.model, provider.version, provider.dimensions, query)
        if key in self._queries:
            self._queries.move_to_end(key)
            return self._queries[key]
        try:
            async with asyncio.timeout(1.5):
                vector = await provider.embed(query)
        except (TimeoutError, ValueError):
            return {}
        if vector is None:
            return {}
        options = {"embedding": vector, "model_id": provider.model, "version": provider.version}
        self._queries[key] = options
        if len(self._queries) > 128:
            self._queries.popitem(last=False)
        return options

    def _events(self, access, *, ids=None, before_seq=None, query=None, with_cursor=False):
        cid = access.conversation.key
        self.permissions.require(access, "history.read", "conversation:" + cid)
        with self.db.engine.connect() as conn:
            hidden = set(conn.execute(select(social_tombstones.c.source_id).where(
                social_tombstones.c.conversation_id == cid)).scalars())
            hidden.update(conn.execute(select(memory_source_retention.c.source_id).where(
                memory_source_retention.c.conversation_id == cid)).scalars())
            recalls = set(conn.execute(select(inbox.c.message_id).where(
                inbox.c.conversation_id == cid, inbox.c.event_type == "message.recalled")).scalars())
            stmt = select(inbox.c.seq, inbox.c.event_json).where(inbox.c.conversation_id == cid)
            if ids is not None:
                stmt = stmt.where(inbox.c.id.in_(ids))
            if before_seq is not None:
                stmt = stmt.where(inbox.c.seq < before_seq)
            if query:
                # SQLite LIKE works on Chinese substrings without relying on FTS tokenization.
                from sqlalchemy import or_
                terms = sorted(keywords(query), key=len, reverse=True)[:64]
                stmt = stmt.where(or_(*(inbox.c.event_json.contains(term, autoescape=True) for term in terms)))
            rows = conn.execute(stmt.order_by(inbox.c.seq.desc()).limit(500)).all()
        result = []
        for seq, raw in rows:
            event = InboundEvent.model_validate_json(raw)
            if (event.event_type != "message.created" or event.event_id in hidden
                    or event.platform_message_id in recalls
                    or not self.permissions.check(access, "history.source.read", "event:" + event.event_id).allowed):
                continue
            result.append((seq, event))
        if with_cursor:
            return result, min((seq for seq, _ in rows), default=None), len(rows) == 500
        return result

    async def query(self, access, operation, arguments):
        if not isinstance(arguments, dict):
            raise ValueError("arguments must be an object")  # noqa: TRY004 - internal API uses validation errors
        limit = arguments.get("limit", 8)
        if type(limit) is not int or not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        scanned_cursor, more = None, False
        if operation == "history.read":
            ids = arguments.get("source_ids")
            if not isinstance(ids, list) or not 1 <= len(ids) <= 20 or any(not isinstance(v, str) for v in ids):
                raise ValueError("source_ids must contain 1 to 20 IDs")
            events = self._events(access, ids=ids)
        elif operation == "history.search":
            query = arguments.get("query", "")
            if not isinstance(query, str) or not 1 <= len(query) <= 500:
                raise ValueError("query must contain 1 to 500 characters")
            before_seq = arguments.get("before_seq")
            if before_seq is not None and (type(before_seq) is not int or before_seq < 1):
                raise ValueError("invalid history cursor")
            events, scanned_cursor, more = self._events(access, before_seq=before_seq, query=query, with_cursor=True)
            tokens = keywords(query)
            events = [(seq, event) for seq, event in events if tokens & keywords(event.text)]
        elif operation in {"memory.search", "memory.get"}:
            query = arguments.get("query", "")
            if not isinstance(query, str) or len(query) > 500:
                raise ValueError("invalid memory query")
            options = await self.query_options(access, query) if operation == "memory.search" else {}
            records = self.social.retrieve(access, query, limit=limit, **options)
            if operation == "memory.get":
                memory_id = arguments.get("memory_id")
                if not isinstance(memory_id, str) or not memory_id:
                    raise ValueError("memory_id required")
                # Read-by-ID still runs the complete visibility, validity and evidence filters.
                records = [record for record in self.social.visible(access) if record.memory_id == memory_id]
            return {"records": [record.model_dump(mode="json") for record in records],
                    "source_ids": sorted({source for record in records for source in record.source_event_ids}),
                    "retrieval": "hybrid" if options else "lexical"}
        else:
            raise ValueError("unsupported memory operation")
        selected = events[:limit]
        return {"messages": [{"source_id": event.event_id, "seq": seq,
                "message_id": event.platform_message_id, "speaker": event.principal.key,
                "text": event.text, "at": event.occurred_at.isoformat(), "reply_to": event.reply_to,
                "segments": [segment.model_dump(mode="json") for segment in event.segments]}
                for seq, event in selected], "source_ids": [event.event_id for _, event in selected],
                "before_seq": min((seq for seq, _ in selected), default=None) if len(events) > limit else scanned_cursor,
                "truncated": len(events) > limit or more}
