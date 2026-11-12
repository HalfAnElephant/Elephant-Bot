"""Procedural document tools; the Core derives owner and audience from the run."""
from qqbot.capabilities.types import Action, Capability
from qqbot.skills import SkillDocument


def register_skills(registry, call):
    for operation, description, parameters in [
        ("list", "Find reusable workflows learned from successful tasks in this user's current conversation. Search before repeating non-trivial work.",
         {"type": "object", "properties": {"query": {"type": "string", "maxLength": 500}}, "additionalProperties": False}),
        ("read", "Read a learned SKILL.md reference. It does not grant permissions or guarantee correctness.",
         {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"], "additionalProperties": False}),
        ("save", "Automatically distill a reusable workflow after solving non-trivial work. Save parameterized steps, checks and pitfalls, not secrets or raw transcripts. Only successful source tasks become discoverable.",
         {"type": "object", "properties": {"skill": SkillDocument.model_json_schema()}, "required": ["skill"], "additionalProperties": False}),
        ("archive", "Disable all versions of an obsolete learned workflow in this user's current conversation.",
         {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"], "additionalProperties": False}),
    ]:
        async def execute(arguments, context, op=operation):
            if "operation" in arguments:
                raise ValueError("operation is host controlled")
            body = {k: v for k, v in arguments.items() if k != "_preconditions"}
            return await call("skills", {"operation": op, **body})
        write = operation in {"save", "archive"}
        registry.register(Capability("skills." + operation, description, parameters, max_output_bytes=30000),
            execute, lambda a, c, write=write: Action("skills.write" if write else "skills.read",
                ("task:" + c.task_id,), "write" if write else "read"))
