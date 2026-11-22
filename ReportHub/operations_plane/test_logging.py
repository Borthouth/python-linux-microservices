import io
import json
import logging
from unittest.mock import patch

from django.test import SimpleTestCase

from app.core.celery_app import format_celery_logs
from config.logging import JsonLogFormatter


class StructuredLoggingTests(SimpleTestCase):
    def test_chained_exception_is_one_json_line_with_original_severity(self):
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        handler.setFormatter(JsonLogFormatter())
        logger = logging.Logger('exception-test')
        logger.addHandler(handler)
        try:
            try:
                raise ValueError('first cause')
            except ValueError as exc:
                raise RuntimeError('second cause') from exc
        except RuntimeError:
            logger.exception('Multiline exception\nwith a warning and debug text')
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        event = json.loads(output.getvalue())
        self.assertEqual(event['severity'], 'ERROR')
        self.assertIn('ValueError: first cause', event['message'])
        self.assertIn('RuntimeError: second cause', event['message'])
        self.assertIn('\n', event['message'])

    def test_celery_signal_formats_task_context_and_exception(self):
        output = io.StringIO()
        handler = logging.StreamHandler(output)
        logger = logging.Logger('celery-test')
        logger.addHandler(handler)
        format_celery_logs(logger=logger)
        with patch('config.logging.current_task') as task:
            task.request.id = 'task-123'
            task.name = 'reports.generate'
            task.request.delivery_info = {'routing_key': 'queue_warm'}
            logger.warning('First line\nsecond line')
        event = json.loads(output.getvalue())
        self.assertEqual(event['task_id'], 'task-123')
        self.assertEqual(event['queue'], 'queue_warm')
        self.assertEqual(event['task_name'], 'reports.generate')
        self.assertEqual(event['severity'], 'WARNING')
        self.assertEqual(len(output.getvalue().splitlines()), 1)
