import sys
import json
import logging
from datetime import datetime, timezone
from typing import IO
from pipewatch.alerts import Alert

logger = logging.getLogger(__name__)


def console_handler(alert: Alert, stream: IO = sys.stderr) -> None:
    """Print alert to stderr with a timestamp."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stream.write(f"{ts} {alert}\n")
    stream.flush()


def json_handler(alert: Alert, stream: IO = sys.stdout) -> None:
    """Emit alert as a JSON line."""
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rule": alert.rule_name,
        "severity": alert.severity,
        "message": alert.message,
        "pipeline_id": alert.metric.pipeline_id,
        "error_rate": round(alert.metric.error_rate, 6),
        "total_records": alert.metric.total_records,
        "failed_records": alert.metric.failed_records,
    }
    stream.write(json.dumps(payload) + "\n")
    stream.flush()


def logging_handler(alert: Alert) -> None:
    """Forward alert to Python's logging system."""
    msg = str(alert)
    if alert.severity == "critical":
        logger.critical(msg)
    else:
        logger.warning(msg)
