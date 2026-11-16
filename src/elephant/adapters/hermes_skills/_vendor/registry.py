"""Keep upstream schema registration local; Elephant performs permission checks."""
from .host import tool_error

__all__ = ["registry", "tool_error"]


class _Registry:
    def __init__(self):
        self.entries = {}

    def register(self, *, name, schema, handler, **metadata):
        self.entries[name] = (schema, handler)


registry = _Registry()
