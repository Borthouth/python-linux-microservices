import json
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.test import RequestFactory, SimpleTestCase

from control_plane.views.dlq import dlq_requeue_view
from control_plane.views.dashboard import update_widget_preferences


class DlqRequeueViewTests(SimpleTestCase):
    def test_htmx_response_escapes_task_id(self):
        task_id = '"><script>alert(1)</script>'
        request = RequestFactory().post(
            "/dlq/task/requeue/", HTTP_HX_REQUEST="true"
        )
        request.user = get_user_model()(is_staff=True)

        with patch("control_plane.views.dlq.get_redis_client") as get_redis_client:
            get_redis_client.return_value = Mock()
            response = dlq_requeue_view(request, task_id)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.content.decode(),
            '<div id="dlq-row-&quot;&gt;&lt;script&gt;alert(1)&lt;/script&gt;" '
            'class="hidden"></div>',
        )

    def test_requeue_error_does_not_expose_exception_details(self):
        request = RequestFactory().post("/dlq/task/requeue/")
        request.user = get_user_model()(is_staff=True)

        with patch("control_plane.views.dlq.get_redis_client") as get_redis_client:
            get_redis_client.side_effect = RuntimeError("sensitive backend details")
            with self.assertLogs("control_plane.views.dlq", level="ERROR"):
                response = dlq_requeue_view(request, "task-id")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            json.loads(response.content),
            {"status": "error", "message": "Unable to re-queue task."},
        )


class WidgetPreferenceViewTests(SimpleTestCase):
    def test_update_error_does_not_expose_exception_details(self):
        request = RequestFactory().post(
            "/api/widgets/update/",
            data=json.dumps({"widget": "stats", "visible": False}),
            content_type="application/json",
        )
        request.user = get_user_model()(is_staff=True)

        with patch(
            "control_plane.views.dashboard.UserWidgetPreference.objects.get_or_create",
            side_effect=RuntimeError("sensitive database details"),
        ):
            with self.assertLogs("control_plane.views.dashboard", level="ERROR"):
                response = update_widget_preferences(request)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            json.loads(response.content),
            {
                "status": "error",
                "message": "Unable to update widget preferences.",
            },
        )
