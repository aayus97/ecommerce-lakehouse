import logging
import json
import os
import traceback
from datetime import datetime, timezone

try:
    from pythonjsonlogger import jsonlogger
except ImportError:  # pragma: no cover - dependency is declared in requirements.txt
    jsonlogger = None


LOG_FIELDS = [
    "run_id",
    "step",
    "duration_seconds",
    "error_type",
    "stack_trace",
    "failed_input_path",
    "failed_output_path",
    "attempt",
    "return_code",
    "pipeline_name",
]

RESERVED_LOG_RECORD_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "message",
    "module",
    "msecs",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "taskName",
    "thread",
    "threadName",
}


class StructuredJsonFormatter(
    jsonlogger.JsonFormatter if jsonlogger else logging.Formatter
):
    def format(self, record):
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created,
                timezone.utc,
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": getattr(record, "run_id", None) or os.getenv("PIPELINE_RUN_ID"),
            "step": getattr(record, "step", None),
        }

        for field in LOG_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value

        if record.exc_info:
            error = record.exc_info[1]
            payload["error_type"] = error.__class__.__name__
            payload["stack_trace"] = "".join(
                traceback.format_exception(*record.exc_info)
            )

        for key, value in record.__dict__.items():
            if key not in RESERVED_LOG_RECORD_FIELDS and key not in payload:
                payload[key] = value

        return json.dumps(payload, default=str)


def get_logger(name: str):
    os.makedirs("logs", exist_ok=True)

    log_file = f"logs/pipeline_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = StructuredJsonFormatter()

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    return logger
