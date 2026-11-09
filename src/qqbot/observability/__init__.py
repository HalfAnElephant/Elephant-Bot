import json
import logging
from datetime import UTC, datetime


class JSONFormatter(logging.Formatter):
    def format(self, record):
        data = {"at":datetime.now(UTC).isoformat(), "level":record.levelname,
                "component":record.name, "event":record.getMessage()}
        if hasattr(record, "data"):
            data["data"] = record.data
        return json.dumps(data, ensure_ascii=False)


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    logger = logging.getLogger("qqbot")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
