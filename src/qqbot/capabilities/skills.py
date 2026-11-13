"""Native Hermes schemas; identity, owner directory and authorization remain host-bound."""
from qqbot.adapters.hermes_skills import TOOL_SCHEMAS
from qqbot.capabilities.types import Action, Capability


def register_skills(registry, call):
    for name, schema in TOOL_SCHEMAS.items():
        async def execute(arguments, context, tool_name=name):
            body = {key: value for key, value in arguments.items() if key != '_preconditions'}
            return await call('skills', {'operation': tool_name, 'arguments': body})
        write = name == 'skill_manage'
        registry.register(Capability(name, schema['description'], schema['parameters'], max_output_bytes=120000),
                          execute, lambda args, context, write=write: Action(
                              'skills.write' if write else 'skills.read', ('task:' + context.task_id,),
                              'write' if write else 'read'))
