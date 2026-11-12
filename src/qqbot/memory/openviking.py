"""Compatibility import for the sole authoritative OpenViking memory adapter."""
from qqbot.adapters.openviking import OpenVikingMemory
from qqbot.adapters.openviking.service import digest

__all__ = ["OpenVikingMemory", "digest"]
