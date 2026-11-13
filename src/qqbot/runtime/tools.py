"""Structured task tools; all identity and resource scope is host-injected."""
from qqbot.domain.agents import ChildSpec, GoalSpec, ProgressSpec, TaskInputSpec, WaitSpec


def task_schemas():
    definitions = {
        "tasks.submit": ("Accept a user goal with initial Workers, reusing matching active work. Immediately acknowledge naturally, then end the Leader turn without waiting or polling.", GoalSpec.model_json_schema()),
        "tasks.spawn": ("Create an independent work/consult child or linked rework in this task tree.", ChildSpec.model_json_schema()),
        "tasks.wait": ("Persist all/any dependency on this task's children, safely yield and release execution capacity.", WaitSpec.model_json_schema()),
        "tasks.take_over": ("Take over the bound goal after all Workers have stopped and external operations are reconciled. Enables direct business execution for this decision turn.",
                            {"type": "object", "properties": {}, "additionalProperties": False}),
        "tasks.send_input": ("Communicate new user information or revised constraints to existing work with followup; answer a real open question with answer and its question_id; notify carries information without revising the goal.", TaskInputSpec.model_json_schema()),
        "tasks.report_progress": ("Report an evidence-backed milestone; plans and heartbeats are not verified progress.", ProgressSpec.model_json_schema()),
        "tasks.status": ("Read a task's true execution, verification and delivery state.",
                         {"type": "object", "properties": {"task_ref": {"type": "string"}}, "required": ["task_ref"], "additionalProperties": False}),
        "tasks.cancel": ("Revoke this authorized task tree and stop its active work.",
                         {"type": "object", "properties": {"task_ref": {"type": "string"}}, "required": ["task_ref"], "additionalProperties": False}),
        "tasks.list": ("List tasks in the current authorized scope.", {"type": "object", "properties": {}, "additionalProperties": False}),
    }
    return {name: {"name": name, "description": description, "parameters": schema}
            for name, (description, schema) in definitions.items()}


def native_tools(role, purpose):
    from qqbot.adapters.hermes_skills import TOOL_SCHEMAS
    result = dict(TOOL_SCHEMAS)
    if role == "skill_review":
        return result
    allowed = {"tasks.status", "tasks.list"}
    if role == "worker":
        allowed |= {"tasks.spawn", "tasks.wait", "tasks.send_input", "tasks.cancel", "tasks.report_progress"}
    elif purpose == "user":
        allowed |= {"tasks.submit", "tasks.send_input", "tasks.cancel"}
    elif purpose in {"finalize", "attention"}:
        allowed |= {"tasks.spawn", "tasks.wait", "tasks.cancel", "tasks.take_over"}
    if role == "leader" and purpose != "skill_review":
        properties = {
            "memory_search": ("Search scoped long-term memory references.", {"query": {"type": "string", "maxLength": 4000}}, ["query"]),
            "memory_read": ("Read an actual returned memory URI as untrusted reference data.", {"uri": {"type": "string"}}, ["uri"]),
        }
        if purpose == "user":
            properties.update({
                "memory_correct": ("Correct a known memory using this real user's current statement.", {"uri": {"type": "string"}}, ["uri"]),
                "memory_forget": ("Forget explicitly identified memories or original source IDs; block stale recalls immediately.",
                    {"uris": {"type": "array", "items": {"type": "string"}, "maxItems": 128},
                     "source_event_ids": {"type": "array", "items": {"type": "string"}, "maxItems": 128}}, []),
            })
        for name, (description, props, required) in properties.items():
            result[name] = {"name": name, "description": description, "parameters": {
                "type": "object", "properties": props, "required": required, "additionalProperties": False}}
        if purpose == "user":
            from qqbot.runtime.schedules import ScheduleSpec
            result["schedules.create"] = {"name": "schedules.create",
                "description": "Schedule this user's authorized goal at an aware date/time or a five-field cron in an IANA timezone. Persist once and report the returned next fire time.",
                "parameters": ScheduleSpec.model_json_schema()}
            for operation, description, fields, required in (
                ("list", "List this authenticated user's schedules.", {}, []),
                ("get", "Read a schedule and its current revision before changing it.",
                    {"schedule_id": {"type": "string"}}, ["schedule_id"]),
                ("update", "Update or pause a schedule using its latest revision.",
                    {"schedule_id": {"type": "string"}, "expected_revision": {"type": "integer", "minimum": 1},
                     "changes": {"type": "object"}}, ["schedule_id", "expected_revision", "changes"]),
                ("cancel", "Cancel future triggers of this user's schedule. Existing tasks keep their own lifecycle.",
                    {"schedule_id": {"type": "string"}, "expected_revision": {"type": "integer", "minimum": 1}},
                    ["schedule_id", "expected_revision"]),
            ):
                name = "schedules." + operation
                result[name] = {"name": name, "description": description, "parameters": {
                    "type": "object", "properties": fields, "required": required, "additionalProperties": False}}
    result.update({name: schema for name, schema in task_schemas().items() if name in allowed})
    return result
