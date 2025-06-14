import json
import time
from logging import Formatter

from django.conf import settings

LEVELS_MAP = {
    "INFO": "info",
    "WARNING": "warning",
    "ERROR": "error",
    "CRITICAL": "fatal",
}


class CustomFormatter(Formatter):
    def format(self, record):
        super().format(record)
        log_data = {
            "app": settings.APP_NAME,
            "anteonenv": settings.ANTEON_ENV,
            "level": LEVELS_MAP[record.levelname],
            "time": record.asctime,
        }

        if hasattr(record, 'metric'):
            log_data = log_data | {
                "metric": record.metric,
            }
        if hasattr(record, 'error'):
            log_data = log_data | {
                "error_reason": record.error,
                "status_code": 500,
            }
        if hasattr(record, 'status_code'):
            log_data = log_data | {
                "status_code": record.status_code,
            }
        if hasattr(record, 'job_id') and record.job_id != None and record.job_id != "":
            log_data = log_data | {
                "job_id": record.job_id,
            }
        if hasattr(record, 'message') and record.message != None and record.message != "":
            log_data = log_data | {
                "message": record.message,
            }
        if hasattr(record, 'exc_info') and record.exc_info != None and record.exc_info != "":
            if not record.exc_text:
                exc_text = self.formatException(record.exc_info)
            else:
                exc_text = record.exc_text
            log_data = log_data | {
                "message": exc_text,
            }
        if record.name == "django.server":
            log_data = log_data | {
                "http_method": record.args[0].split(" ")[0],
                "endpoint": record.args[0].split(" ")[1],
            }
        return json.dumps(log_data)

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        datefmt = '%Y-%m-%dT%H:%M:%SZ'
        return time.strftime(datefmt, ct)
