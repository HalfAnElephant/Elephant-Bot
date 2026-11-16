"""Compatibility import for the sole authoritative OpenViking memory adapter."""
from elephant.adapters.openviking import OpenVikingMemory
from elephant.adapters.openviking.service import digest

__all__ = ["OpenVikingMemory", "digest"]
