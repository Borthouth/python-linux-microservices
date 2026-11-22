import json
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID, uuid4

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import Client, SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from operations_plane.log_store import _where
from operations_plane.models import (
    OperationsLogAnalysis,
    OperationsLogSearch,
    OperationsUser,
    OperationsWidgetPreference,
)


class OperationsLogStoreTests(SimpleTestCase):
    def test_filters_target_the_opentelemetry_log_schema(self):
        range_end = timezone.now()
        where, parameters = _where({
            'range_start': range_end - timedelta(hours=1),
            'range_end': range_end,
            'service': 'redis',
            'severity': 'ERROR',
            'queue': 'queue_warm',
            'task_name': 'reports.tasks.generate',
            'task_id': 'task-123',
            'worker': 'warm_worker',
            'text': 'connection failed',
        })

        self.assertIn('Timestamp >=', where)
        self.assertIn('ServiceName =', where)
        self.assertIn('SeverityText =', where)
        self.assertIn("LogAttributes['queue'] =", where)
        self.assertIn("LogAttributes['task_name'] =", where)
        self.assertIn("LogAttributes['task_id'] =", where)
        self.assertIn("LogAttributes['worker'] =", where)
        self.assertIn('positionCaseInsensitive(Body,', where)
        self.assertEqual(parameters['service'], 'redis')
        self.assertEqual(parameters['task_id'], 'task-123')
        self.assertEqual(parameters['text'], 'connection failed')


@override_settings(
    OPERATIONS_HOST='',
    CACHES={
        'default': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        },
    },
    ALLOWED_HOSTS=[
        'testserver',
        'control.example.test',
        'operations.example.test',
    ]
)
class OperationsAuthenticationTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.control_plane_user = user_model.objects.create_user(
            username='keith',
            password='ControlPlane-password-92!',
        )
        self.operations_auth_user = user_model.objects.create_user(
            username=str(uuid4()),
            password='Operations-password-92!',
            is_staff=False,
            is_superuser=False,
        )
        self.operations_user = OperationsUser.objects.create(
            auth_user=self.operations_auth_user,
            username='keith',
            role=OperationsUser.Role.SUPPORT,
        )

    def test_operations_login_is_separate_from_control_plane_username(self):
        self.assertNotEqual(
            self.control_plane_user.pk,
            self.operations_user.auth_user_id,
        )
        self.assertEqual(self.control_plane_user.username, self.operations_user.username)
        UUID(self.operations_user.auth_user.username)

        response = self.client.post(
            reverse('operations_plane:login'),
            {'username': 'keith', 'password': 'Operations-password-92!'},
            HTTP_HOST='operations.example.test',
        )

        self.assertRedirects(
            response,
            reverse('operations_plane:dashboard'),
            fetch_redirect_response=False,
        )
        self.operations_auth_user.refresh_from_db()
        self.assertIsNotNone(self.operations_auth_user.last_login)

        dashboard = self.client.get(
            reverse('operations_plane:dashboard'),
            HTTP_HOST='operations.example.test',
        )
        self.assertContains(dashboard, 'Operations Plane')
        self.assertContains(dashboard, 'Support Staff')
        self.assertContains(dashboard, 'Last successful login')

    def test_control_plane_password_cannot_authenticate_operations_account(self):
        response = self.client.post(
            reverse('operations_plane:login'),
            {'username': 'keith', 'password': 'ControlPlane-password-92!'},
            HTTP_HOST='operations.example.test',
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Invalid username or password.')
        self.assertNotIn('_auth_user_id', self.client.session)

    def test_tailwind_play_cdn_is_only_loaded_in_debug(self):
        cdn_url = 'https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4'
        with override_settings(DEBUG=True):
            debug_response = self.client.get(reverse('operations_plane:login'))
        self.assertContains(debug_response, cdn_url)

        with override_settings(DEBUG=False):
            production_response = self.client.get(reverse('operations_plane:login'))
        self.assertNotContains(production_response, cdn_url)

    def test_operations_credentials_cannot_sign_in_to_control_plane(self):
        response = self.client.post(
            '/control-plane/login/',
            {
                'username': self.operations_auth_user.username,
                'password': 'Operations-password-92!',
            },
            HTTP_HOST='control.example.test',
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Invalid username or password.')
        self.assertNotIn('_auth_user_id', self.client.session)

    @override_settings(OPERATIONS_HOST='operations.example.test')
    def test_configured_operations_host_uses_isolated_root_routes(self):
        response = self.client.get('/', HTTP_HOST='operations.example.test')
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['Location'], '/login/')

        response = self.client.post(
            '/login/',
            {'username': 'keith', 'password': 'Operations-password-92!'},
            HTTP_HOST='operations.example.test',
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response['Location'], '/')
        self.assertContains(
            self.client.get('/', HTTP_HOST='operations.example.test'),
            'Operations Plane',
        )

        self.assertEqual(
            self.client.get(
                '/control-plane/login/',
                HTTP_HOST='operations.example.test',
            ).status_code,
            404,
        )
        self.assertEqual(
            self.client.get(
                '/operations/',
                HTTP_HOST='control.example.test',
            ).status_code,
            404,
        )

    def test_operations_user_history_is_stored_in_its_own_model_history(self):
        self.operations_user.role = OperationsUser.Role.ADMIN
        self.operations_user.save(update_fields=['role'])

        history_model = OperationsUser.history.model
        self.assertEqual(history_model._meta.app_label, 'operations_plane')
        self.assertEqual(
            history_model._meta.db_table,
            'operations_plane_historicaloperationsuser',
        )
        self.assertTrue(
            history_model.objects.filter(
                id=self.operations_user.pk,
                role=OperationsUser.Role.ADMIN,
            ).exists()
        )

    def test_unknown_and_inactive_accounts_are_rejected(self):
        unknown_response = self.client.post(
            reverse('operations_plane:login'),
            {'username': 'missing', 'password': 'wrong-password'},
        )
        self.assertContains(unknown_response, 'Invalid username or password.')

        self.operations_user.is_active = False
        self.operations_user.save(update_fields=['is_active'])
        inactive_response = self.client.post(
            reverse('operations_plane:login'),
            {'username': 'keith', 'password': 'Operations-password-92!'},
        )
        self.assertContains(inactive_response, 'Invalid username or password.')
        self.assertNotIn('_auth_user_id', self.client.session)

    def test_dashboard_requires_an_operations_account(self):
        response = self.client.get(reverse('operations_plane:dashboard'))

        self.assertRedirects(
            response,
            reverse('operations_plane:login'),
            fetch_redirect_response=False,
        )

        self.client.force_login(self.control_plane_user)
        response = self.client.get(reverse('operations_plane:dashboard'))
        self.assertRedirects(
            response,
            reverse('operations_plane:login'),
            fetch_redirect_response=False,
        )

    def test_logout_requires_post_and_ends_operations_session(self):
        self.client.force_login(self.operations_auth_user)

        get_response = self.client.get(reverse('operations_plane:logout'))
        self.assertEqual(get_response.status_code, 405)

        response = self.client.post(reverse('operations_plane:logout'))
        self.assertRedirects(
            response,
            reverse('operations_plane:login'),
            fetch_redirect_response=False,
        )
        dashboard = self.client.get(reverse('operations_plane:dashboard'))
        self.assertRedirects(
            dashboard,
            reverse('operations_plane:login'),
            fetch_redirect_response=False,
        )

    @patch('operations_plane.management.commands.create_operations_user.getpass')
    def test_provision_command_creates_non_staff_uuid_auth_user(self, getpass):
        getpass.side_effect = ['Strong-admin-pass-92!', 'Strong-admin-pass-92!']

        call_command(
            'create_operations_user',
            '--username',
            'keith-admin',
            '--email',
            'ops@example.test',
            '--role',
            OperationsUser.Role.ADMIN,
        )

        account = OperationsUser.objects.get(username='keith-admin')
        self.assertEqual(account.role, OperationsUser.Role.ADMIN)
        self.assertEqual(account.email, 'ops@example.test')
        UUID(account.auth_user.username)
        self.assertFalse(account.auth_user.is_staff)
        self.assertFalse(account.auth_user.is_superuser)
        self.assertTrue(account.auth_user.check_password('Strong-admin-pass-92!'))

    def test_login_session_cookie_is_host_only(self):
        control_client = Client()
        control_response = control_client.post(
            '/control-plane/login/',
            {'username': 'keith', 'password': 'ControlPlane-password-92!'},
            HTTP_HOST='control.example.test',
        )
        self.assertEqual(control_response.status_code, 302)

        operations_client = Client()
        operations_response = operations_client.post(
            reverse('operations_plane:login'),
            {'username': 'keith', 'password': 'Operations-password-92!'},
            HTTP_HOST='operations.example.test',
        )
        self.assertEqual(operations_response.status_code, 302)
        self.assertEqual(
            control_response.cookies[settings.SESSION_COOKIE_NAME]['domain'],
            '',
        )
        self.assertEqual(
            operations_response.cookies[settings.SESSION_COOKIE_NAME]['domain'],
            '',
        )


@override_settings(
    OPERATIONS_HOST='',
    CACHES={
        'default': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
        },
    },
    ALLOWED_HOSTS=[
        'testserver',
        'control.example.test',
        'operations.example.test',
    ],
)
class OperationsLogViewTests(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.auth_user = user_model.objects.create_user(
            username=str(uuid4()),
            password='Operations-password-92!',
        )
        self.operations_user = OperationsUser.objects.create(
            auth_user=self.auth_user,
            username='log-support',
            role=OperationsUser.Role.SUPPORT,
        )
        self.client.force_login(self.auth_user)

    @staticmethod
    def _date_value(value):
        return timezone.localtime(value).strftime('%Y-%m-%dT%H:%M')

    def test_recent_log_screen_renders_filters_and_task_drilldown(self):
        now = timezone.now().replace(second=0, microsecond=0)
        row = {
            'event_id': str(uuid4()),
            'event_time': now,
            'ingested_at': now,
            'service': 'celery',
            'severity': 'ERROR',
            'message': 'Task failed',
            'task_id': str(uuid4()),
            'task_name': 'reports.tasks.generate',
            'queue': 'queue_warm',
            'worker': 'reporthub_celery_warm_worker',
            'source': 'stderr',
        }
        with (
            patch('operations_plane.log_views.latest_ingestion_time', return_value=now),
            patch(
                'operations_plane.log_views.search_logs',
                return_value={'rows': [row], 'has_more': False},
            ) as search,
        ):
            response = self.client.get(
                reverse('operations_plane:logs'),
                {
                    'range_start': self._date_value(now - timedelta(hours=1)),
                    'range_end': self._date_value(now),
                },
                HTTP_HOST='operations.example.test',
            )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'Log search')
        self.assertContains(response, 'queue_warm')
        self.assertContains(response, 'reports.tasks.generate')
        self.assertContains(response, 'timeline')
        search.assert_called_once()
        self.assertEqual(search.call_args.args[0]['range_start'].tzinfo, timezone.get_current_timezone())

    def test_historical_search_is_queued_and_results_are_owner_scoped(self):
        now = timezone.now().replace(second=0, microsecond=0)
        search_time = now - timedelta(days=2)
        parameters = {
            'range_start': self._date_value(search_time),
            'range_end': self._date_value(search_time + timedelta(hours=1)),
            'queue': 'queue_warm',
            'source': 'celery',
        }
        with patch(
            'operations_plane.log_views.run_log_search.apply_async',
            return_value=SimpleNamespace(id='celery-search-1'),
        ) as enqueue:
            response = self.client.get(
                reverse('operations_plane:logs'),
                parameters,
                HTTP_HOST='operations.example.test',
            )

        search = OperationsLogSearch.objects.get(requested_by=self.operations_user)
        self.assertRedirects(
            response,
            reverse('operations_plane:log_search_detail', args=[search.pk]),
            fetch_redirect_response=False,
        )
        self.assertEqual(search.status, OperationsLogSearch.Status.QUEUED)
        self.assertEqual(search.queue_filter, 'queue_warm')
        self.assertEqual(search.source_filter, 'celery')
        self.assertEqual(search.task_id, 'celery-search-1')
        enqueue.assert_called_once_with(args=[search.pk], queue='queue_operations_logs')

        other_auth_user = get_user_model().objects.create_user(
            username=str(uuid4()),
            password='Other-operations-password-92!',
        )
        OperationsUser.objects.create(
            auth_user=other_auth_user,
            username='other-support',
            role=OperationsUser.Role.SUPPORT,
        )
        self.client.force_login(other_auth_user)
        response = self.client.get(
            reverse('operations_plane:log_search_detail', args=[search.pk]),
            HTTP_HOST='operations.example.test',
        )
        self.assertEqual(response.status_code, 404)

    def test_analyze_action_is_explicit_and_uses_ai_queue(self):
        now = timezone.now().replace(second=0, microsecond=0)
        search = OperationsLogSearch.objects.create(
            requested_by=self.operations_user, range_start=now - timedelta(hours=3),
            range_end=now, status=OperationsLogSearch.Status.COMPLETED,
            service_filter='celery', severity_filter='ERROR', source_filter='celery',
        )
        with patch(
            'operations_plane.log_views.run_log_analysis.apply_async',
            return_value=SimpleNamespace(id='celery-analysis-1'),
        ) as enqueue, self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                reverse('operations_plane:analyze_logs'),
                {'search_id': search.pk},
                HTTP_HOST='operations.example.test',
            )

        analysis = OperationsLogAnalysis.objects.get(requested_by=self.operations_user)
        self.assertRedirects(
            response,
            reverse('operations_plane:log_search_detail', args=[search.pk])
            + f'?analysis={analysis.pk}#ai-analysis',
            fetch_redirect_response=False,
        )
        self.assertEqual(analysis.status, OperationsLogAnalysis.Status.QUEUED)
        self.assertEqual(analysis.service_filter, 'celery')
        self.assertEqual(analysis.severity_filter, 'ERROR')
        self.assertEqual(analysis.source_filter, 'celery')
        enqueue.assert_called_once_with(args=[analysis.pk], queue='queue_operations_ai')

    def test_flash_messages_have_a_dismiss_button(self):
        response = self.client.post(
            reverse('operations_plane:analyze_logs'),
            {},
            HTTP_HOST='operations.example.test',
        )
        self.assertRedirects(
            response,
            reverse('operations_plane:logs'),
            fetch_redirect_response=False,
        )

        response = self.client.get(
            reverse('operations_plane:logs'),
            HTTP_HOST='operations.example.test',
        )
        self.assertContains(response, 'Complete a log search')
        self.assertContains(response, 'aria-label="Dismiss message"')
        self.assertContains(response, 'data-dismiss-message')

    def test_dashboard_widget_preferences_are_saved_per_operations_user(self):
        response = self.client.post(
            reverse('operations_plane:update_widget_preferences'),
            data=json.dumps({'widget': 'searches', 'visible': False}),
            content_type='application/json',
            HTTP_HOST='operations.example.test',
        )

        self.assertEqual(response.status_code, 200)
        preference = OperationsWidgetPreference.objects.get(
            operations_user=self.operations_user,
        )
        self.assertFalse(preference.show_searches)

        dashboard = self.client.get(
            reverse('operations_plane:dashboard'),
            HTTP_HOST='operations.example.test',
        )
        self.assertContains(
            dashboard,
            'id="widget-searches-wrapper" class="hidden',
        )
