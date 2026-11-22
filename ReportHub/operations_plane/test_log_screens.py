from datetime import timedelta
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from django.contrib.auth import get_user_model
from django.test import Client, SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from operations_plane.log_store import LOG_FIELDS, LogStoreUnavailable, recent_logs, search_logs
from operations_plane.models import OperationsLogAnalysis, OperationsLogSearch, OperationsLogSources, OperationsUser
from operations_plane.source_status import collection_status
from operations_plane.tasks import (
    AI_OPTIONS, MAX_AI_PROMPT_BYTES, _filters_for, _sample_groups,
    _serialize_log_rows, _summarize_log_data, run_log_search,
    run_log_analysis,
)


@override_settings(OPERATIONS_HOST='')
class LogScreenTests(TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        override = self.settings(OPERATIONS_LOGGING_CONTROL_DIR=self.directory.name)
        override.enable()
        self.addCleanup(override.disable)
        self.user = get_user_model().objects.create_user(username=str(uuid4()))
        self.account = OperationsUser.objects.create(
            auth_user=self.user, username='screen-support', role=OperationsUser.Role.SUPPORT,
        )
        self.client.force_login(self.user)
        self.now = timezone.now()
        self.message = '<script>alert("untrusted")</script>\nsecond\nthird\n' + 'x' * 1000 + 'END_OF_FULL_MESSAGE'
        self.row = {
            'event_id': str(uuid4()), 'event_time': self.now, 'ingested_at': self.now,
            'service': 'debezium-connect', 'severity': 'ERROR', 'message': self.message,
            'source_category': 'debezium', 'source': 'container', 'logger': 'probe',
            'queue': '', 'worker': '', 'task_id': '', 'task_name': '',
            'partial_id': '', 'partial_ordinal': '', 'partial_last': '',
        }

    @patch('operations_plane.log_store._client')
    @patch('operations_plane.views.latest_ingestion_time')
    def test_dashboard_connectivity_card_links_to_logs_without_fetching_entries(self, latest, client_factory):
        cases = (
            (self.now, 'Connected', 'Latest event received'),
            (None, 'Connected', 'waiting for its first log event'),
            (LogStoreUnavailable, 'Unavailable', 'The log store could not be reached'),
        )
        for result, state, detail in cases:
            with self.subTest(state=state, empty=result is None):
                latest.side_effect = result if result is LogStoreUnavailable else None
                latest.return_value = result
                latest.reset_mock()
                response = self.client.get(reverse('operations_plane:dashboard'))
                self.assertContains(response, 'Store connectivity')
                self.assertNotContains(response, 'widget-recent_logs-wrapper')
                self.assertNotContains(response, 'data-widget-toggle="recent_logs"')
                self.assertNotContains(response, 'Source ingestion settings:')
                card = response.content.decode().split('id="widget-log_store-wrapper"', 1)[1].split('</section>', 1)[0]
                self.assertIn(state, card)
                self.assertIn(detail, card)
                for name in ('live_logs', 'logs'):
                    self.assertIn(f'href="{reverse(f"operations_plane:{name}")}"', card)
                latest.assert_called_once_with()
                client_factory.assert_not_called()
        response = self.client.post(
            reverse('operations_plane:update_widget_preferences'),
            data=json.dumps({'widget': 'recent_logs', 'visible': True}),
            content_type='application/json',
        )
        self.assertEqual(response.status_code, 400)

    @patch('operations_plane.log_views.recent_logs')
    def test_live_feed_validates_bounds_and_keeps_source_filter_independent_of_ingestion(self, recent):
        url = reverse('operations_plane:live_logs_data')
        for parameters in ({'limit': 101}, {'limit': 0}, {'source': 'invented'}, {'since': 'invalid'}):
            with self.subTest(parameters=parameters):
                self.assertEqual(self.client.get(url, parameters).status_code, 400)
        recent.assert_not_called()
        recent.return_value = {'rows': [self.row], 'has_more': True}
        response = self.client.get(url, {'limit': 50, 'source': 'debezium'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(recent.call_args.kwargs['limit'], 50)
        self.assertEqual(recent.call_args.args[0]['source'], 'debezium')
        self.assertNotIn('END_OF_FULL_MESSAGE', response.json()['html'])
        self.assertIn('&lt;script&gt;', response.json()['html'])
        self.assertIn('View full message', response.json()['html'])
        self.assertTrue(response.json()['has_more'])
        self.assertEqual(response.json()['cursor'], self.now.isoformat())
        self.assertIn('no-store', response['Cache-Control'])

    @patch('operations_plane.log_views.recent_logs', side_effect=LogStoreUnavailable)
    def test_feed_outage_is_an_error_not_an_empty_success(self, recent):
        response = self.client.get(reverse('operations_plane:live_logs_data'))
        self.assertEqual(response.status_code, 503)
        self.assertIn('unavailable', response.json()['error'])

    @patch('operations_plane.log_views.run_log_analysis.apply_async', return_value=SimpleNamespace(id='analysis-default-range'))
    @patch('operations_plane.log_views.search_logs', return_value={'rows': [], 'has_more': False})
    @patch('operations_plane.log_views.latest_ingestion_time')
    def test_ai_action_requires_a_successful_search_and_retains_applied_filters(self, latest, search, enqueue):
        latest.return_value = self.now
        url = reverse('operations_plane:logs')
        page = self.client.get(url)
        self.assertNotContains(page, 'Analyze saved results with AI')
        self.assertEqual(page.context['form_values']['source'], '')
        search.assert_not_called()
        invalid = self.client.get(url, {'range_start': 'invalid'})
        self.assertNotContains(invalid, 'Analyze saved results with AI')
        search.assert_not_called()
        parameters = {
            'range_start': (self.now - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M'),
            'range_end': self.now.strftime('%Y-%m-%dT%H:%M'),
            'source': 'debezium',
            'text': 'snapshot',
        }
        page = self.client.get(url, parameters)
        self.assertContains(page, 'Analyze saved results with AI')
        self.assertNotContains(page, 'value="None"')
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                reverse('operations_plane:analyze_logs'),
                {'search_id': page.context['search'].pk},
            )
        analysis = OperationsLogAnalysis.objects.get(requested_by=self.account)
        self.assertRedirects(
            response,
            reverse('operations_plane:log_search_detail', args=[analysis.search_id])
            + f'?analysis={analysis.pk}#ai-analysis',
            fetch_redirect_response=False,
        )
        self.assertEqual(analysis.source_filter, 'debezium')
        self.assertEqual(analysis.text_filter, 'snapshot')
        self.assertEqual(analysis.severity_filter, '')
        self.assertEqual(analysis.service_filter, '')
        enqueue.assert_called_once_with(args=[analysis.pk], queue='queue_operations_ai')
        search.side_effect = LogStoreUnavailable
        unavailable = self.client.get(url, parameters)
        self.assertNotContains(unavailable, 'Analyze saved results with AI')

    @patch('operations_plane.log_views.run_log_analysis.apply_async', return_value=SimpleNamespace(id='analysis-historical-range'))
    def test_historical_search_offers_ai_only_after_completion_with_saved_filters(self, enqueue):
        start = (self.now - timedelta(days=2)).replace(second=0, microsecond=0)
        search = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=start, range_end=start + timedelta(hours=1),
            source_filter='kafka', service_filter='kafka', severity_filter='WARNING',
        )
        url = reverse('operations_plane:log_search_detail', args=[search.pk])
        for state in (search.Status.QUEUED, search.Status.RUNNING, search.Status.FAILED):
            with self.subTest(state=state):
                search.status = state
                search.save()
                self.assertNotContains(self.client.get(url), 'Analyze saved results with AI')
        search.status = search.Status.COMPLETED
        search.total_events = 0
        search.save()
        page = self.client.get(url)
        self.assertContains(page, 'Analyze saved results with AI')
        with self.captureOnCommitCallbacks(execute=True):
            self.client.post(reverse('operations_plane:analyze_logs'), {'search_id': search.pk})
        analysis = OperationsLogAnalysis.objects.get(requested_by=self.account)
        self.assertEqual(analysis.range_start, search.range_start)
        self.assertEqual(analysis.range_end, search.range_end)
        self.assertEqual(analysis.source_filter, 'kafka')
        self.assertEqual(analysis.service_filter, 'kafka')
        self.assertEqual(analysis.severity_filter, 'WARNING')
        enqueue.assert_called_once_with(args=[analysis.pk], queue='queue_operations_ai')

    @patch('operations_plane.log_views.run_log_analysis.apply_async', return_value=SimpleNamespace(id='linked-ai'))
    def test_inline_analysis_uses_saved_filters_deduplicates_and_restores_owned_runs(self, enqueue):
        search = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=self.now - timedelta(hours=1),
            range_end=self.now, status=OperationsLogSearch.Status.COMPLETED,
            text_filter='saved filter',
        )
        url = reverse('operations_plane:analyze_logs')
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                url, {'search_id': search.pk, 'text': 'tampered'}, HTTP_ACCEPT='application/json',
            )
        analysis = search.analyses.get()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(analysis.text_filter, 'saved filter')
        self.assertEqual(response.json()['status'], 'QUEUED')
        self.assertIn('Queued', response.json()['html'])
        self.assertIn('no-store', response['Cache-Control'])
        with self.captureOnCommitCallbacks(execute=True):
            duplicate = self.client.post(url, {'search_id': search.pk}, HTTP_ACCEPT='application/json')
        self.assertEqual(duplicate.json()['id'], analysis.pk)
        enqueue.assert_called_once()
        analysis.status = analysis.Status.RUNNING
        analysis.save()
        status_url = response.json()['status_url']
        self.assertIn('Processing', self.client.get(status_url).json()['html'])
        analysis.status = analysis.Status.COMPLETED
        analysis.summary = '<script>untrusted</script> saved summary'
        analysis.save()
        result = self.client.get(status_url)
        self.assertIn('&lt;script&gt;', result.json()['html'])
        self.assertEqual(result.json()['status_label'], 'Completed')
        self.assertIn('no-store', result['Cache-Control'])
        detail = reverse('operations_plane:log_search_detail', args=[search.pk])
        self.assertContains(self.client.get(detail, {'analysis': analysis.pk}), 'saved summary')
        history = self.client.get(reverse('operations_plane:log_search_history'))
        self.assertContains(history, 'Completed (1)')
        self.assertNotContains(history, '?analysis=')
        self.assertRedirects(
            self.client.get(reverse('operations_plane:log_analysis_detail', args=[analysis.pk])),
            detail + f'?analysis={analysis.pk}#ai-analysis', fetch_redirect_response=False,
        )
        other_user = get_user_model().objects.create_user(username=str(uuid4()))
        other = OperationsUser.objects.create(auth_user=other_user, username='other-screen')
        self.client.force_login(other_user)
        self.assertEqual(self.client.get(status_url).status_code, 404)
        self.assertEqual(self.client.post(url, {'search_id': search.pk}, HTTP_ACCEPT='application/json').status_code, 404)
        foreign_search = OperationsLogSearch.objects.create(
            requested_by=other, range_start=search.range_start, range_end=search.range_end,
            status=search.Status.COMPLETED,
        )
        self.assertEqual(self.client.get(
            reverse('operations_plane:log_search_detail', args=[foreign_search.pk]),
            {'analysis': analysis.pk},
        ).status_code, 404)

    @patch('operations_plane.log_views.run_log_analysis.apply_async')
    def test_inline_analysis_rejects_missing_invalid_and_unfinished_searches(self, enqueue):
        url = reverse('operations_plane:analyze_logs')
        for value in ('', 'bad', '-1', '9' * 200):
            self.assertEqual(self.client.post(url, {'search_id': value}, HTTP_ACCEPT='application/json').status_code, 400)
        search = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=self.now - timedelta(hours=1), range_end=self.now,
        )
        for state in (search.Status.QUEUED, search.Status.RUNNING, search.Status.FAILED):
            search.status = state
            search.save()
            self.assertEqual(self.client.post(url, {'search_id': search.pk}, HTTP_ACCEPT='application/json').status_code, 404)
        enqueue.assert_not_called()

    @patch('operations_plane.log_views.run_log_analysis.apply_async')
    def test_completed_ai_is_shown_only_for_the_same_saved_snapshot_and_filters(self, enqueue):
        parameters = {
            'requested_by': self.account, 'range_start': self.now - timedelta(hours=1),
            'range_end': self.now, 'status': OperationsLogSearch.Status.COMPLETED,
            'source_filter': 'kafka',
        }
        original = OperationsLogSearch.objects.create(**parameters)
        another_page = OperationsLogSearch.objects.create(**parameters)
        completed = OperationsLogAnalysis.objects.create(
            requested_by=self.account, search=original, range_start=original.range_start,
            range_end=self.now, source_filter='kafka', status=OperationsLogAnalysis.Status.COMPLETED,
            summary='Reusable completed summary',
            saved_result_filters={'q': '', 'severity': '', 'source': ''},
        )
        failed = OperationsLogAnalysis.objects.create(
            requested_by=self.account, search=original, range_start=original.range_start,
            range_end=self.now, source_filter='kafka', status=OperationsLogAnalysis.Status.FAILED,
            saved_result_filters={'q': '', 'severity': '', 'source': ''},
        )
        url = reverse('operations_plane:log_search_detail', args=[original.pk])
        response = self.client.get(url)
        self.assertEqual(response.context['analysis'], completed)
        self.assertContains(response, 'Reusable completed summary')
        self.assertContains(response, 'Run a new analysis')
        self.assertContains(response, 'no new AI run was requested')
        self.assertNotContains(response, '>Analyze saved results with AI</button>')
        self.assertEqual(self.client.get(url, {'analysis': failed.pk}).context['analysis'], failed)
        response = self.client.get(reverse('operations_plane:log_search_detail', args=[another_page.pk]))
        self.assertIsNone(response.context['analysis'])
        self.assertNotContains(response, 'Reusable completed summary')
        self.assertEqual(OperationsLogAnalysis.objects.count(), 2)
        enqueue.assert_not_called()

    @patch('operations_plane.tasks.generate_text', return_value='Only selected saved events.')
    @patch('operations_plane.tasks.analysis_log_data')
    @patch('operations_plane.log_views.run_log_analysis.apply_async', return_value=SimpleNamespace(id='saved-ai'))
    def test_filtered_ai_uses_all_matching_saved_pages_and_switching_restores_run_inputs(self, enqueue, clickhouse, generate):
        stored = [
            {**self.row, 'event_id': str(uuid4()), 'source_category': 'debezium',
             'message': 'Selected needle' if index >= 10 else 'Not selected',
             'severity': 'ERROR' if index >= 10 else 'INFO'}
            for index in range(40)
        ]
        search = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=self.now - timedelta(hours=1),
            range_end=self.now, status=OperationsLogSearch.Status.COMPLETED,
            results=_serialize_log_rows(stored),
        )
        post_url = reverse('operations_plane:analyze_logs')
        filters = {'q': 'needle', 'severity': 'ERROR', 'source': 'debezium'}
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(post_url, {'search_id': search.pk, **filters}, HTTP_ACCEPT='application/json')
        filtered = search.analyses.get()
        self.assertEqual(filtered.saved_result_filters, filters)
        self.assertIn(f'analysis={filtered.pk}', response.json()['runs_html'])
        self.assertIn('Filtered saved events', response.json()['runs_html'])
        self.assertIn('Filtered saved events', response.json()['html'])
        self.assertNotIn('Unfiltered saved events', response.json()['html'])
        run_log_analysis(filtered.pk)
        filtered.refresh_from_db()
        self.assertEqual(filtered.total_events, 30)
        self.assertEqual(filtered.sampled_events, 30)
        self.assertEqual(filtered.breakdown, [{'service': self.row['service'], 'severity': 'ERROR', 'count': 30}])
        self.assertEqual(filtered.status, filtered.Status.COMPLETED)
        clickhouse.assert_not_called()
        self.assertIn('Selected needle', generate.call_args.args[0])
        self.assertNotIn('Not selected', generate.call_args.args[0])
        generate.return_value = 'Unfiltered saved events summary.'
        with self.captureOnCommitCallbacks(execute=True):
            self.client.post(post_url, {'search_id': search.pk}, HTTP_ACCEPT='application/json')
        unfiltered = search.analyses.exclude(pk=filtered.pk).get()
        run_log_analysis(unfiltered.pk)
        unfiltered.refresh_from_db()
        self.assertEqual(unfiltered.total_events, 40)
        self.assertEqual(unfiltered.saved_result_filters, {'q': '', 'severity': '', 'source': ''})
        detail = reverse('operations_plane:log_search_detail', args=[search.pk])
        filtered_page = self.client.get(detail, {'analysis': filtered.pk})
        self.assertEqual(filtered_page.context['analysis'], filtered)
        self.assertEqual(filtered_page.context['results_form']['q'].value(), 'needle')
        self.assertEqual(filtered_page.context['results_form']['severity'].value(), 'ERROR')
        self.assertEqual(filtered_page.context['results_form']['source'].value(), 'debezium')
        self.assertContains(filtered_page, f'aria-current="true" href="{detail}?analysis={filtered.pk}#ai-analysis"')
        self.assertContains(filtered_page, 'data-ai-result-disclosure')
        self.assertContains(filtered_page, 'not all logs in the original time range')
        self.assertContains(filtered_page, 'the AI does not read every full message')
        html = filtered_page.content.decode()
        self.assertLess(html.index('Analysis scope and limits'), html.index('data-ai-result-disclosure'))
        self.assertNotContains(filtered_page, 'Unfiltered saved events summary.')
        unfiltered_page = self.client.get(detail, {'analysis': unfiltered.pk})
        self.assertEqual(unfiltered_page.context['results_form']['q'].value(), '')
        self.assertContains(unfiltered_page, 'Unfiltered saved events summary.')
        self.assertNotContains(unfiltered_page, 'Only selected saved events.')
        for run, label, other_label in (
            (filtered, 'Filtered saved events', 'Unfiltered saved events'),
            (unfiltered, 'Unfiltered saved events', 'Filtered saved events'),
        ):
            payload = self.client.get(reverse('operations_plane:log_analysis_status', args=[run.pk])).json()
            self.assertIn(label, payload['html'])
            self.assertNotIn(other_label, payload['html'])
            self.assertIn('not every full message', payload['html'])
        self.assertEqual(self.client.post(post_url, {'search_id': search.pk, 'source': 'invented'}, HTTP_ACCEPT='application/json').status_code, 400)
        clickhouse.assert_not_called()

    @patch('operations_plane.log_views.latest_ingestion_time')
    @patch('operations_plane.log_views.search_logs')
    def test_recent_pagination_preserves_filters_and_saves_the_result_page(self, query, latest):
        latest.return_value = self.now
        query.return_value = {'rows': [self.row], 'has_more': True}
        parameters = {
            'range_start': (self.now - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M'),
            'range_end': self.now.strftime('%Y-%m-%dT%H:%M'), 'source': 'debezium', 'text': 'a & b',
        }
        url = reverse('operations_plane:logs')
        response = self.client.get(url, parameters)
        self.assertRegex(response.content.decode(), r'</section>\s*<nav aria-label="Results pagination"')
        saved = response.context['search']
        self.assertEqual(saved.results, _serialize_log_rows([self.row]))
        self.assertEqual(saved.status, saved.Status.COMPLETED)
        self.assertFalse(response.context['pagination']['previous_url'])
        next_url = response.context['pagination']['next_url']
        self.assertIn('text=a+%26+b', next_url)
        response = self.client.get(next_url)
        self.assertEqual(query.call_args.kwargs['before'], (self.now, self.row['event_id']))
        previous_url = response.context['pagination']['previous_url']
        query.return_value = {'rows': [self.row], 'has_more': False}
        response = self.client.get(previous_url)
        self.assertEqual(query.call_args.kwargs['after'], (self.now, self.row['event_id']))
        self.assertFalse(response.context['pagination']['previous_url'])
        self.assertTrue(response.context['pagination']['next_url'])
        query.reset_mock()
        for cursor in ({'event_id': self.row['event_id']}, {'event_time': 'bad', 'event_id': self.row['event_id']}):
            self.assertContains(self.client.get(url, {**parameters, **cursor}), 'page cursor is invalid')
        query.assert_not_called()

    @patch('operations_plane.log_views.run_log_search.apply_async', return_value=SimpleNamespace(id='page-search'))
    @patch('operations_plane.tasks.search_logs')
    def test_background_newer_cursor_is_saved_and_used_by_worker(self, query, enqueue):
        parameters = {
            'range_start': (self.now - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M'),
            'range_end': (self.now - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M'),
            'event_time': self.now.isoformat(), 'event_id': self.row['event_id'], 'direction': 'newer',
        }
        self.client.get(reverse('operations_plane:logs'), parameters)
        search = OperationsLogSearch.objects.get()
        self.assertEqual(search.after_event_time, self.now)
        query.return_value = {'rows': [self.row], 'has_more': False}
        run_log_search(search.pk)
        self.assertEqual(query.call_args.kwargs['after'], (self.now, self.row['event_id']))
        search.refresh_from_db()
        self.assertEqual(search.status, search.Status.COMPLETED)
        self.assertEqual(search.results, _serialize_log_rows([self.row]))

    def test_existing_large_saved_results_display_only_25_with_continuation(self):
        search = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=self.now - timedelta(hours=1),
            range_end=self.now, status=OperationsLogSearch.Status.COMPLETED,
            results=_serialize_log_rows([self.row] * 100), total_events=100,
        )
        response = self.client.get(reverse('operations_plane:log_search_detail', args=[search.pk]))
        self.assertEqual(len(response.context['rows']), 25)
        self.assertNotContains(response, '25 events on this page')
        self.assertContains(response, 'Search saved results')
        self.assertNotContains(response, '>Clear filters</a>')
        self.assertTrue(response.context['pagination']['next_url'])
        self.assertFalse(response.context['pagination']['previous_url'])
        self.assertIn(f'/searches/{search.pk}/?q=', response.context['pagination']['next_url'])
        next_page = self.client.get(response.context['pagination']['next_url'])
        self.assertEqual(len(next_page.context['rows']), 25)
        self.assertEqual(OperationsLogSearch.objects.count(), 1)
        submitted = self.client.get(
            reverse('operations_plane:log_search_detail', args=[search.pk]),
            {'q': '', 'severity': '', 'source': ''},
        )
        self.assertContains(submitted, '>Clear filters</a>')

    @patch('operations_plane.log_views.search_logs')
    @patch('operations_plane.log_views.latest_ingestion_time')
    def test_saved_result_filters_search_the_entire_snapshot_before_paginating_without_clickhouse(self, latest, query):
        stored_rows = [
            {
                **self.row, 'event_id': str(uuid4()),
                'message': 'x' * 400 + (' Hidden Needle' if index >= 30 else ' other'),
                'severity': 'ERROR' if index >= 30 else 'INFO',
                'source_category': 'kafka' if index >= 30 else 'debezium',
            } for index in range(60)
        ]
        search = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=self.now - timedelta(hours=1),
            range_end=self.now, status=OperationsLogSearch.Status.COMPLETED,
            results=_serialize_log_rows(stored_rows), total_events=60, has_more=True,
        )
        analysis = OperationsLogAnalysis.objects.create(
            requested_by=self.account, search=search, range_start=search.range_start,
            range_end=self.now, status=OperationsLogAnalysis.Status.COMPLETED,
            summary='Existing analysis stays unchanged',
        )
        original_results = search.results
        url = reverse('operations_plane:log_search_detail', args=[search.pk])
        response = self.client.get(url, {
            'q': 'NEEDLE', 'severity': 'ERROR', 'source': 'kafka', 'analysis': analysis.pk,
        })
        self.assertEqual(len(response.context['rows']), 25)
        self.assertEqual(response.context['rows'][0]['event_id'], stored_rows[30]['event_id'])
        self.assertContains(response, 'Existing analysis stays unchanged')
        next_url = response.context['pagination']['next_url']
        for value in ('q=NEEDLE', 'severity=ERROR', 'source=kafka', f'analysis={analysis.pk}', 'page=2'):
            self.assertIn(value, next_url)
        second = self.client.get(next_url)
        self.assertEqual(len(second.context['rows']), 5)
        self.assertFalse(second.context['pagination']['next_url'])
        self.assertTrue(second.context['pagination']['previous_url'])
        self.assertContains(self.client.get(url, {'q': 'absent phrase'}), 'No saved events match these filters.')
        for values in ({'severity': 'invented'}, {'source': 'invented'}, {'page': 0}):
            self.assertEqual(self.client.get(url, values).status_code, 400)
        self.assertEqual(self.client.get(url, {'page': 99}).status_code, 404)
        search.refresh_from_db()
        analysis.refresh_from_db()
        self.assertEqual(search.results, original_results)
        self.assertEqual(analysis.summary, 'Existing analysis stays unchanged')
        self.assertEqual(OperationsLogSearch.objects.count(), 1)
        latest.assert_not_called()
        query.assert_not_called()

    def test_history_delete_is_confirmed_post_only_and_deletes_linked_ai_results(self):
        search = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=self.now - timedelta(hours=1),
            range_end=self.now, status=OperationsLogSearch.Status.COMPLETED,
        )
        for status in (OperationsLogAnalysis.Status.COMPLETED, OperationsLogAnalysis.Status.FAILED):
            OperationsLogAnalysis.objects.create(
                requested_by=self.account, search=search, range_start=search.range_start,
                range_end=self.now, status=status, summary='Linked saved AI output',
            )
        unlinked = OperationsLogAnalysis.objects.create(
            requested_by=self.account, range_start=search.range_start, range_end=self.now,
            status=OperationsLogAnalysis.Status.COMPLETED,
        )
        history = self.client.get(reverse('operations_plane:log_search_history'))
        self.assertContains(history, f'aria-label="View results for search {search.pk}"')
        self.assertContains(history, f'aria-label="Delete saved search {search.pk}"')
        url = reverse('operations_plane:delete_log_search', args=[search.pk])
        self.assertEqual(self.client.get(url).status_code, 405)
        unconfirmed = self.client.post(url, {}, follow=True)
        self.assertContains(unconfirmed, 'Confirm deletion')
        self.assertTrue(OperationsLogSearch.objects.filter(pk=search.pk).exists())
        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(self.user)
        self.assertEqual(csrf_client.post(url, {'confirm': 'yes'}).status_code, 403)
        response = self.client.post(url, {'confirm': 'yes'}, follow=True)
        self.assertContains(response, 'Saved search and linked AI results deleted.')
        self.assertFalse(OperationsLogSearch.objects.filter(pk=search.pk).exists())
        self.assertEqual(list(OperationsLogAnalysis.objects.values_list('pk', flat=True)), [unlinked.pk])

    def test_history_deletion_rejects_other_owners_and_active_jobs(self):
        search = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=self.now - timedelta(hours=1), range_end=self.now,
        )
        url = reverse('operations_plane:delete_log_search', args=[search.pk])
        for status in (search.Status.QUEUED, search.Status.RUNNING):
            search.status = status
            search.save()
            self.assertContains(self.client.post(url, {'confirm': 'yes'}, follow=True), 'still active')
            self.assertTrue(OperationsLogSearch.objects.filter(pk=search.pk).exists())
        search.status = search.Status.COMPLETED
        search.save()
        analysis = OperationsLogAnalysis.objects.create(
            requested_by=self.account, search=search, range_start=search.range_start, range_end=self.now,
        )
        for status in (analysis.Status.QUEUED, analysis.Status.RUNNING):
            analysis.status = status
            analysis.save()
            self.assertContains(self.client.post(url, {'confirm': 'yes'}, follow=True), 'still active')
            self.assertTrue(OperationsLogAnalysis.objects.filter(pk=analysis.pk).exists())
        other_user = get_user_model().objects.create_user(username=str(uuid4()))
        OperationsUser.objects.create(auth_user=other_user, username='delete-other')
        self.client.force_login(other_user)
        self.assertEqual(self.client.post(url, {'confirm': 'yes'}).status_code, 404)
        self.client.logout()
        self.assertRedirects(
            self.client.post(url, {'confirm': 'yes'}),
            reverse('operations_plane:login'), fetch_redirect_response=False,
        )

    def test_history_summarizes_ai_status_counts_without_individual_run_links(self):
        search = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=self.now - timedelta(hours=1),
            range_end=self.now, status=OperationsLogSearch.Status.COMPLETED,
        )
        url = reverse('operations_plane:log_search_history')
        self.assertContains(self.client.get(url), 'Not requested')
        for status in (
            OperationsLogAnalysis.Status.COMPLETED,
            OperationsLogAnalysis.Status.COMPLETED,
            OperationsLogAnalysis.Status.QUEUED,
            OperationsLogAnalysis.Status.RUNNING,
            OperationsLogAnalysis.Status.FAILED,
        ):
            OperationsLogAnalysis.objects.create(
                search=search, requested_by=self.account, status=status,
                range_start=search.range_start, range_end=search.range_end,
            )
        response = self.client.get(url)
        for label in ('Completed (2)', 'Queued (1)', 'Processing (1)', 'Failed (1)'):
            self.assertContains(response, label, count=1)
        self.assertNotContains(response, 'Not requested')
        self.assertNotContains(response, '?analysis=')
        self.assertContains(response, 'View results')

    def test_history_is_owner_scoped_searchable_and_paginated_with_status(self):
        for index in range(21):
            OperationsLogSearch.objects.create(
                requested_by=self.account, range_start=self.now - timedelta(hours=1),
                range_end=self.now, text_filter='history needle',
                status=OperationsLogSearch.Status.COMPLETED,
            )
        failed = OperationsLogSearch.objects.create(
            requested_by=self.account, range_start=self.now - timedelta(hours=1),
            range_end=self.now, status=OperationsLogSearch.Status.FAILED,
        )
        other_user = get_user_model().objects.create_user(username=str(uuid4()))
        other = OperationsUser.objects.create(auth_user=other_user, username='history-other')
        OperationsLogSearch.objects.create(
            requested_by=other, range_start=failed.range_start, range_end=self.now,
            text_filter='private history',
        )
        url = reverse('operations_plane:log_search_history')
        page = self.client.get(url, {'q': 'history needle', 'status': 'COMPLETED'})
        self.assertEqual(len(page.context['page_obj']), 20)
        self.assertNotContains(page, 'private history')
        self.assertNotContains(page, f'Search #{failed.pk}<')
        self.assertRegex(page.content.decode(), r'</section>\s*<div class="mt-6 flex items-center justify-between bg-gray-900')
        second = self.client.get(page.context['pagination']['next_url'])
        self.assertEqual(len(second.context['page_obj']), 1)
        self.assertIn('status=COMPLETED', second.context['pagination']['previous_url'])
        self.assertEqual(list(self.client.get(url, {'q': str(failed.pk)}).context['page_obj']), [failed])
        self.assertEqual(len(self.client.get(url, {'q': '9' * 200}).context['page_obj']), 0)
        for values in ({'status': 'INVALID'}, {'page': 0}):
            self.assertEqual(self.client.get(url, values).status_code, 400)
        self.assertEqual(self.client.get(url, {'page': 200}).status_code, 404)

    @patch('operations_plane.log_views.log_event')
    def test_event_details_load_the_complete_message_and_fragment_metadata_on_demand(self, query):
        query.return_value = {**self.row, 'partial_id': 'fragment-id', 'partial_ordinal': '2'}
        url = reverse('operations_plane:log_event', args=[self.row['event_id']])
        self.assertEqual(self.client.get(url).status_code, 400)
        query.assert_not_called()
        response = self.client.get(url, {'event_time': self.now.isoformat()})
        self.assertEqual(response.json()['message'], self.message)
        self.assertEqual(response.json()['partial_id'], 'fragment-id')
        self.assertEqual(query.call_args.args[1], self.now)
        query.return_value = None
        self.assertEqual(self.client.get(url, {'event_time': self.now.isoformat()}).status_code, 404)

    def test_live_page_and_event_api_require_an_operations_account(self):
        page = self.client.get(reverse('operations_plane:live_logs'))
        self.assertContains(page, 'Pause live view')
        self.assertContains(page, 'Log search')
        self.assertNotContains(page, 'No task link')
        self.client.logout()
        for url in (
            reverse('operations_plane:live_logs'),
            reverse('operations_plane:live_logs_data'),
            reverse('operations_plane:log_event', args=[self.row['event_id']]),
        ):
            self.assertRedirects(
                self.client.get(url), reverse('operations_plane:login'),
                fetch_redirect_response=False,
            )

    @patch('operations_plane.log_views.search_logs')
    @patch('operations_plane.log_views.latest_ingestion_time')
    def test_log_pages_share_the_store_status_component_without_navigation(self, latest, search):
        latest.return_value = self.now
        search.return_value = {'rows': [], 'has_more': False}
        component = 'operations_plane/logs/components/log_store_status.html'
        for name in ('live_logs', 'logs'):
            with self.subTest(page=name):
                response = self.client.get(reverse(f'operations_plane:{name}'))
                self.assertTemplateUsed(response, component)
                self.assertContains(response, 'data-log-store-status', count=1)
                html = response.content.decode()
                card = html.split('<div data-log-store-status', 1)[1].split('</div>', 1)[0]
                self.assertNotIn('<a ', card)
                self.assertIn('Checking connection' if name == 'live_logs' else 'Connected', card)
        latest.side_effect = LogStoreUnavailable
        response = self.client.get(reverse('operations_plane:logs'))
        self.assertTemplateUsed(response, component)
        self.assertContains(response, 'role="status">Unavailable')

    def test_source_status_respects_transport_gate_and_stale_confirmation(self):
        selection = OperationsLogSources.objects.create(
            pk=1, revision=uuid4(), enabled_sources=['debezium'],
        )
        path = Path(self.directory.name) / 'status.json'
        path.write_text(json.dumps({
            'revision': str(selection.revision), 'heartbeat': self.now.timestamp(),
        }))
        status = collection_status()
        self.assertTrue(status['applied'])
        debezium = next(s for s in status['categories'] if s['key'] == 'debezium')
        self.assertEqual(debezium['state'], 'Ingestion disabled')
        self.assertFalse(debezium['enabled'])
        path.write_text(json.dumps({'revision': str(selection.revision), 'heartbeat': 0}))
        self.assertTrue(all(s['state'] == 'Unconfirmed' for s in collection_status()['categories']))

    def test_background_jobs_retain_source_category(self):
        record = SimpleNamespace(
            range_start=self.now - timedelta(days=3), range_end=self.now,
            source_filter='kafka', service_filter='', severity_filter='',
            queue_filter='', task_name_filter='', task_id_filter='', worker_filter='', text_filter='',
        )
        self.assertEqual(_filters_for(record)['source'], 'kafka')


class RecentLogQueryTests(SimpleTestCase):
    @patch('operations_plane.log_store._client')
    def test_search_pages_default_to_25_and_cannot_exceed_25(self, client_factory):
        now = timezone.now()
        values = {field: '' for field in LOG_FIELDS}
        values.update(event_id=str(uuid4()), event_time=now, ingested_at=now, message='probe')
        client = client_factory.return_value
        client.query.return_value = SimpleNamespace(
            result_rows=[tuple(values[field] for field in LOG_FIELDS)] * 26,
        )
        filters = {'range_start': now - timedelta(hours=1), 'range_end': now}
        for kwargs in ({}, {'limit': 100}):
            with self.subTest(kwargs=kwargs):
                result = search_logs(filters, **kwargs)
                self.assertEqual(len(result['rows']), 25)
                self.assertTrue(result['has_more'])
                self.assertIn('LIMIT 26', client.query.call_args.args[0])

    @patch('operations_plane.log_store._client')
    def test_newer_pages_take_nearest_rows_and_reverse_to_newest_first(self, client_factory):
        now = timezone.now()
        ids = ['00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000003']
        values = {field: '' for field in LOG_FIELDS}
        values.update(event_time=now, ingested_at=now, message='probe')
        client = client_factory.return_value
        client.query.return_value = SimpleNamespace(result_rows=[
            tuple({**values, 'event_id': event_id}[field] for field in LOG_FIELDS) for event_id in ids
        ])
        result = search_logs(
            {'range_start': now - timedelta(hours=1), 'range_end': now},
            after=(now, ids[0]), limit=2,
        )
        self.assertEqual([row['event_id'] for row in result['rows']], list(reversed(ids[:2])))
        self.assertTrue(result['has_more'])
        sql = client.query.call_args.args[0]
        self.assertIn("event_id) >", sql)
        self.assertIn('event_id ASC', sql)
        self.assertIn('LIMIT 3', sql)

    @patch('operations_plane.log_store._client')
    def test_receipt_time_overlap_and_result_limit_are_enforced(self, client_factory):
        now = timezone.now()
        since = now - timedelta(seconds=10)
        client = Mock()
        client_factory.return_value = client
        values = {field: '' for field in LOG_FIELDS}
        values.update(event_id=str(uuid4()), event_time=now, ingested_at=now, message='probe')
        client.query.return_value = SimpleNamespace(
            result_rows=[tuple(values[field] for field in LOG_FIELDS)] * 102,
        )
        result = recent_logs({'source': 'kafka'}, limit=200, since=since)
        sql = client.query.call_args.args[0]
        parameters = client.query.call_args.kwargs['parameters']
        self.assertIn('ingested_at >=', sql)
        self.assertIn('ORDER BY ingested_at DESC, event_id DESC LIMIT 101', sql)
        self.assertEqual(parameters['range_start'], since - timedelta(seconds=2))
        self.assertEqual(parameters['source'], 'kafka')
        self.assertEqual(len(result['rows']), 100)
        self.assertTrue(result['has_more'])
        client.close.assert_called_once()


class LogAnalysisPromptTests(SimpleTestCase):
    @patch('operations_plane.tasks.generate_text', return_value='Supported pattern.')
    def test_repeated_samples_are_grouped_without_losing_coverage(self, generate):
        sample = {'service': 'worker', 'severity': 'ERROR', 'message': 'Repeated failure'}
        samples = [{**sample, 'task_id': str(index), 'event_time': str(index)} for index in range(300)]
        groups = _sample_groups(samples)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]['sample_occurrences'], 300)
        summary = _summarize_log_data({'samples': samples, 'sampled_events': 300, 'total_events': 4000, 'breakdown': []})
        self.assertIn('300 of 4000', summary)
        generate.assert_called_once()
        self.assertEqual(generate.call_args.kwargs['options'], AI_OPTIONS)
        self.assertLessEqual(len(generate.call_args.args[0].encode('utf-8')), MAX_AI_PROMPT_BYTES)

    @patch('operations_plane.tasks.generate_text', return_value='A' * 3000)
    def test_unique_unicode_and_escaped_samples_and_reduction_obey_byte_budget(self, generate):
        samples = [
            {'service': 'x' * 300, 'message': str(index) + '\u2603\x00"\\' * 1000}
            for index in range(12)
        ]
        _summarize_log_data({'samples': samples, 'sampled_events': 12, 'total_events': 12, 'breakdown': []})
        self.assertGreater(generate.call_count, 1)
        for call in generate.call_args_list:
            self.assertLessEqual(len(call.args[0].encode('utf-8')), MAX_AI_PROMPT_BYTES)
            self.assertEqual(call.kwargs['options'], AI_OPTIONS)
        generate.return_value = ' '
        with self.assertRaisesMessage(ValueError, 'empty log summary'):
            _summarize_log_data({'samples': samples[:1], 'sampled_events': 1, 'total_events': 1})
