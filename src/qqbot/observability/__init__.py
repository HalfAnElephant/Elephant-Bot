import json
import logging
import traceback
from datetime import UTC, datetime


class JSONFormatter(logging.Formatter):
    def format(self, record):
        data = {"at":datetime.now(UTC).isoformat(), "level":record.levelname,
                "component":record.name, "event":record.getMessage()}
        if hasattr(record, "data"):
            data["data"] = record.data
        for key in ("message_id", "conversation", "principal", "task_id", "run_id", "request_id",
                    "operation_id", "worker_id", "process_id"):
            value = getattr(record, key, None)
            if isinstance(value, (str, int)):
                data[key] = value
        if record.exc_info:
            data["error_type"] = record.exc_info[0].__name__
            data["traceback"] = [
                {"file": frame.filename, "line": frame.lineno, "function": frame.name}
                for frame in traceback.extract_tb(record.exc_info[2])
            ]
        return json.dumps(data, ensure_ascii=False)


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    logger = logging.getLogger("qqbot")
    logger.handlers = [handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False
