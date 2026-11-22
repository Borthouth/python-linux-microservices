import json
import logging
import re
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.core.paginator import Paginator
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.views.decorators.http import require_POST
from control_plane.dlq import DispatchUncertain, REPORT_TASK, RetryRejected, requeue_failure
from control_plane.models import DeadLetterTask
from control_plane.utils import get_redis_client

logger = logging.getLogger(__name__)


def is_admin(user):
    return user.is_staff or user.is_superuser


def _decode_traceback(value):
    if not isinstance(value, str) or not value:
        return value

    # Redis payloads may contain traceback text escaped across multiple layers.
    value = re.sub(r'\\+u(?=[0-9a-fA-F]{4})', lambda _: r'\u', value)
    value = re.sub(r'\\+([nrt"])', lambda match: '\\' + match.group(1), value)

    def decode_surrogate_pair(match):
        high, low = (int(code_unit, 16) for code_unit in match.groups())
        return chr(0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00))

    value = re.sub(
        r'\\u([dD][89a-bA-B][0-9a-fA-F]{2})\\u([dD][c-fC-F][0-9a-fA-F]{2})',
        decode_surrogate_pair,
        value,
    )
    value = re.sub(r'\\u([0-9a-fA-F]{4})', lambda match: chr(int(match.group(1), 16)), value)
    return value.replace(r'\n', '\n').replace(r'\r', '\r').replace(r'\t', '\t').replace(r'\"', '"')


def load_dlq_entries(r):
    keys = r.keys("dlq:task:*")
    records = list(DeadLetterTask.objects.prefetch_related("attempts"))
    seen_ids = {record.original_task_id for record in records}
    tasks = [
        {
            "id": record.pk,
            "display_name": record.task_name,
            "error_message": record.error_message,
            "traceback": record.error_message or "No error details recorded.",
            "failed_timestamp": record.created_at,
            "status": record.status,
            "status_label": record.get_status_display(),
            "retryable": bool(
                record.target_id and (record.task_name != REPORT_TASK or record.original_execution_id)
            ) and record.status in (
                DeadLetterTask.Status.FAILED, DeadLetterTask.Status.FAILED_AGAIN,
            ),
            "attempts": record.attempts.all(),
            "legacy": False,
        }
        for record in records
    ]
    invalid_records = 0
    for key in keys:
        data = r.get(key)
        if not data:
            continue
        try:
            task_dict = json.loads(data)
            if not isinstance(task_dict, dict):
                raise ValueError("DLQ entry is not an object")
            key_text = key.decode("utf-8") if isinstance(key, bytes) else key
            task_id = str(task_dict.get("id") or task_dict.get("task_id") or key_text.rsplit(":", 1)[-1])
            if task_id in seen_ids:
                continue
            seen_ids.add(task_id)
            raw_time = task_dict.get("failed_at") or task_dict.get("timestamp") or ""
            failed_at = parse_datetime(raw_time) if isinstance(raw_time, str) else None
            if failed_at and timezone.is_naive(failed_at):
                failed_at = timezone.make_aware(failed_at)
            raw_traceback = task_dict.get("traceback") or ""
            tasks.append({
                "id": task_id,
                "display_name": task_dict.get("task_name") or task_dict.get("name") or task_id,
                "error_message": task_dict.get("error_summary") or raw_traceback or "Exception raised during execution",
                "traceback": _decode_traceback(str(raw_traceback)) if raw_traceback else "No traceback recorded.",
                "failed_timestamp": failed_at,
                "status": DeadLetterTask.Status.FAILED,
                "status_label": "Failed",
                "retryable": False,
                "legacy": True,
            })
        except (ValueError, TypeError, UnicodeDecodeError):
            invalid_records += 1
            logger.warning("Unable to decode a legacy DLQ record", exc_info=True)
    tasks.sort(
        key=lambda task: task["failed_timestamp"].timestamp() if task["failed_timestamp"] else float("-inf"),
        reverse=True,
    )
    return tasks, invalid_records


@login_required
@user_passes_test(is_admin)
def dlq_list_view(request):
    search_query = request.GET.get("q", "").strip()
    status_filter = request.GET.get("status", "")
    tasks, invalid_records = load_dlq_entries(get_redis_client())
    if invalid_records:
        messages.warning(request, "Some dead-letter records could not be read. Check the application logs.")
    if search_query:
        query_lower = search_query.lower()
        tasks = [
            t for t in tasks
            if query_lower in str(t.get('display_name', '')).lower()
            or query_lower in str(t.get('error_message', '')).lower()
        ]

    if status_filter in DeadLetterTask.Status.values:
        tasks = [task for task in tasks if task["status"] == status_filter]

    paginator = Paginator(tasks, 15)
    page_obj = paginator.get_page(request.GET.get("page", 1))

    context = {
        "page_obj": page_obj,
        "dlq_tasks": page_obj.object_list,
        "search_query": search_query,
        "status_filter": status_filter,
        "filter_options": [
            {"value": value, "label": label} for value, label in DeadLetterTask.Status.choices
        ],
    }

    # Support HTMX partial table swaps on search or pagination
    if request.headers.get("HX-Request"):
        return render(
            request, "control_plane/dlq/components/dlq_table.html", context
        )

    return render(request, "control_plane/dlq_list.html", context)


@login_required
@user_passes_test(is_admin)
@require_POST
def dlq_requeue_view(request, task_id):
    messages.error(request, "This older entry does not contain supported retry details.")
    return redirect("control_plane:dlq_list")


@login_required
@user_passes_test(is_admin)
@require_POST
def dlq_retry_view(request, failure_id):
    failure = get_object_or_404(DeadLetterTask, pk=failure_id)
    try:
        requeue_failure(failure.pk, request.user)
    except (RetryRejected, DispatchUncertain) as exc:
        messages.error(request, str(exc))
    else:
        messages.success(request, "Task submitted for requeueing. Check its status for the outcome.")
    return redirect("control_plane:dlq_list")