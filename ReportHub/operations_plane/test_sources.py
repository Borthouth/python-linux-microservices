import json
from pathlib import Path
import tempfile
import time
from unittest.mock import patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse

from operations_plane.models import OperationsLogSources, OperationsUser
from operations_plane.source_status import collection_status
from otel.source_policy import default_sources, filter_conditions


class LogSourceTests(TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        override = self.settings(OPERATIONS_LOGGING_CONTROL_DIR=self.directory.name)
        override.enable()
        self.addCleanup(override.disable)
        self.auth_user = get_user_model().objects.create_user(username=str(uuid4()))
        self.account = OperationsUser.objects.create(
            auth_user=self.auth_user, username='source-admin', role=OperationsUser.Role.ADMIN,
        )
        self.client.force_login(self.auth_user)
        self.url = reverse('operations_plane:log_sources')

    def test_support_cannot_view_or_change_sources(self):
        self.account.role = OperationsUser.Role.SUPPORT
        self.account.save()
        self.assertEqual(self.client.get(self.url).status_code, 403)
        self.assertEqual(self.client.post(self.url, {'enabled_sources': []}).status_code, 403)
        self.assertFalse(OperationsLogSources.objects.exists())

    def test_save_all_off_is_published_and_audited(self):
        response = self.client.post(self.url, {})
        self.assertEqual(response.status_code, 302)
        selection = OperationsLogSources.objects.get(pk=1)
        self.assertEqual(selection.enabled_sources, [])
        desired = json.loads((Path(self.directory.name) / 'desired.json').read_text())
        self.assertEqual(desired['enabled_sources'], [])
        self.assertEqual(desired['revision'], str(selection.revision))
        self.assertEqual(selection.updated_by, self.auth_user)
        self.assertEqual(selection.history.first().history_user, self.auth_user)

    def test_invalid_source_and_csrf_are_rejected(self):
        response = self.client.post(self.url, {'enabled_sources': ['arbitrary-yaml']})
        self.assertEqual(response.status_code, 200)
        self.assertFalse(OperationsLogSources.objects.exists())
        client = Client(enforce_csrf_checks=True)
        client.force_login(self.auth_user)
        self.assertEqual(client.post(self.url, {}).status_code, 403)

    def test_publish_failure_does_not_save_choices(self):
        with patch('operations_plane.source_views.publish_selection', side_effect=OSError):
            response = self.client.post(self.url, {})
        self.assertContains(response, 'Your changes were not saved')
        self.assertFalse(OperationsLogSources.objects.exists())

    def test_only_fresh_matching_confirmation_is_shown_as_applied(self):
        selection = OperationsLogSources.objects.create(pk=1, revision=uuid4())
        status = Path(self.directory.name) / 'status.json'
        status.write_text(json.dumps({'revision': str(selection.revision), 'heartbeat': time.time()}))
        response = self.client.get(self.url)
        self.assertContains(response, 'Collector confirmed these choices.', count=1)
        self.assertEqual(
            [(message.tags, str(message)) for message in response.context['messages']],
            [('success', 'Collector confirmed these choices.')],
        )
        self.assertNotContains(self.client.get(self.url), 'Collector confirmed these choices')
        status.write_text(json.dumps({'revision': str(selection.revision), 'heartbeat': 0}))
        client = Client()
        client.force_login(self.auth_user)
        self.assertNotContains(client.get(self.url), 'Collector confirmed these choices')

    def test_confirmation_message_waits_for_each_saved_revision(self):
        status = Path(self.directory.name) / 'status.json'
        for enabled in (['redis'], ['fluent', 'redis']):
            self.client.post(self.url, {'enabled_sources': enabled})
            selection = OperationsLogSources.objects.get(pk=1)
            response = self.client.get(self.url)
            self.assertContains(response, 'Waiting for Collector confirmation.')
            self.assertNotContains(response, 'Collector confirmed these choices')
            status.write_text(json.dumps({
                'revision': str(selection.revision), 'heartbeat': time.time(),
            }))
            self.assertContains(self.client.get(self.url), 'Collector confirmed these choices.', count=1)
            self.assertNotContains(self.client.get(self.url), 'Collector confirmed these choices')

    def test_source_policy_rejects_unknown_sources_and_blocks_feedback(self):
        with self.assertRaises(ValueError):
            filter_conditions(['not-a-source'])
        self.assertIn('clickhouse', filter_conditions(default_sources())[0])
        conditions = filter_conditions([key for key in default_sources() if key != 'redis'])
        self.assertTrue(any('reporthub-redis' in condition for condition in conditions))

    def test_logical_sources_show_transport_dependencies_and_generic_labels(self):
        selection = OperationsLogSources.objects.create(
            pk=1, revision=uuid4(), enabled_sources=['etl', 'clamav'],
        )
        response = self.client.get(self.url)
        categories = {category['key']: category for category in collection_status(selection)['categories']}
        self.assertFalse(categories['etl']['enabled'])
        self.assertTrue(categories['clamav']['enabled'])
        self.assertFalse(categories['journal']['enabled'])
        self.assertContains(response, 'Local ETL and ClamAV files are not ingested')
        self.assertNotContains(response, 'ETL log file')
        self.assertNotContains(response, 'ClamAV log files')
