"""OpenViking is the sole long-term fact store; SQLite stores receipts only."""
from .service import OpenVikingMemory

__all__ = ["OpenVikingMemory"]
