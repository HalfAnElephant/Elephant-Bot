"""Bounded wording selection from facts approved by the host."""

import asyncio
import json

import httpx
from sqlalchemy import update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from elephant.credentials_env import credential_env
from elephant.domain import stable_id
from elephant.domain.tasks import validate_user_facing_text
from elephant.providers import output_limit_parameter
from elephant.storage.task_schema import model_requests


async def render_expression(core, *, event, request_id: str, task_id: str,
                            facts: list[list[str]], fallback: str) -> str:
    """The model chooses wording; only exact host-approved sentences reach users."""
    config = core.settings.model
    key = credential_env(config.api_key_env)
    capacity = getattr(getattr(core, "scheduler", None), "capacity", None)
    if (not config.endpoint or not config.model or not key or not 1 <= len(facts) <= 8
            or capacity is None or not core.permissions.check(core.access(event), "model.request",
                "conversation:" + event.conversation.key).allowed
            or any(not 1 <= len(variants) <= 3 or any(
                not isinstance(text, str) or not text.strip() or len(text) > 500
                for text in variants) for variants in facts)):
        return fallback
    identifier = stable_id("host-expression", request_id)
    with core.db.transaction() as conn:
        inserted = conn.execute(sqlite_insert(model_requests).values(
            id=identifier, task_id=task_id, run_id=identifier, kind="host_expression",
            state="RESERVED", created_at=core.clock.now().isoformat())
            .on_conflict_do_nothing(index_elements=[model_requests.c.id]))
        if not inserted.rowcount:
            return fallback
    payload = {"model": config.model, "messages": [
        {"role": "system", "content": "Select one approved sentence variant for each fact, in the given order. "
            "Reply only as JSON: {\"choices\":[0,...]}. Do not add words or omit facts."},
        {"role": "user", "content": json.dumps(facts, ensure_ascii=False)},
    ], "stream": False, output_limit_parameter(config.endpoint): 100}
    state, usage, body = "FAILED", None, fallback
    try:
        async with asyncio.timeout(2):
            async with capacity.slot(background=True):
                if not core.permissions.check(core.access(event), "model.request",
                        "conversation:" + event.conversation.key).allowed:
                    return fallback
                response = await core.model_client.post(config.endpoint, json=payload,
                    headers={"Authorization": "Bearer " + key})
            response.raise_for_status()
            data = response.json()
            selection = json.loads(data["choices"][0]["message"]["content"])
        choices = selection["choices"] if isinstance(selection, dict) and set(selection) == {"choices"} else None
        if (not isinstance(choices, list) or len(choices) != len(facts)
                or any(type(choice) is not int or choice < 0 or choice >= len(variants)
                       for choice, variants in zip(choices, facts))):
            return fallback
        candidate = "\n".join(variants[choice] for choice, variants in zip(choices, facts))
        validate_user_facing_text(candidate)
        body, usage, state = candidate, data.get("usage"), "COMPLETED"
        return body
    except (TimeoutError, httpx.HTTPError, KeyError, IndexError, TypeError, ValueError):
        return fallback
    finally:
        with core.db.transaction() as conn:
            conn.execute(update(model_requests).where(model_requests.c.id == identifier).values(
                state=state, usage_json=json.dumps(usage) if usage is not None else None))
