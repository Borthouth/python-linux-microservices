import json
import logging
from datetime import datetime, timezone

from celery import current_task


class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        message = record.getMessage()
        if record.exc_info:
            message += '\n' + self.formatException(record.exc_info)
        if record.stack_info:
            message += '\n' + self.formatStack(record.stack_info)
        event = {
            'schema': 'reporthub.log.v1',
            'timestamp': datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            'severity': record.levelname,
            'logger': record.name,
            'message': message,
        }
        if current_task and current_task.request.id:
            event.update({
                'task_id': current_task.request.id,
                'task_name': current_task.name,
                'queue': (current_task.request.delivery_info or {}).get('routing_key', ''),
            })
        return json.dumps(event, ensure_ascii=True)
