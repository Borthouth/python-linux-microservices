from contextlib import ExitStack
from functools import wraps

from django.contrib import messages
from django.shortcuts import get_object_or_404, redirect

from control_plane.metadata_guard import SourceMetadataCutoverPending, source_metadata_write_guard
from reports.models import ReportDefinition


def _posted_source_id(request):
    value = request.POST.get("source", "")
    try:
        source_id = int(value)
    except (TypeError, ValueError):
        return None
    return source_id if 0 < source_id < 2**63 else None


def _guarded_report_write(request, view, *, pk=None, source_ids=()):
    try:
        with ExitStack() as stack:
            for source_id in sorted(set(source_ids)):
                stack.enter_context(source_metadata_write_guard(source_id))
            if pk is not None:
                report = get_object_or_404(
                    ReportDefinition.objects.select_for_update(), pk=pk
                )
                if report.source_id not in source_ids:
                    messages.error(request, "The report source changed; reload and try again.")
                    return redirect("reports:report_list")
                return view(request, pk)
            return view(request)
    except SourceMetadataCutoverPending as exc:
        messages.error(request, str(exc))
        return redirect("reports:report_list")


def guard_report_creation(view):
    @wraps(view)
    def wrapped(request):
        source_id = _posted_source_id(request) if request.method == "POST" else None
        if source_id is None:
            return view(request)
        return _guarded_report_write(request, view, source_ids=[source_id])
    return wrapped


def guard_report_mutation(*, post_only=False, may_change_source=False):
    def decorate(view):
        @wraps(view)
        def wrapped(request, pk):
            if post_only and request.method != "POST":
                return view(request, pk)
            report = get_object_or_404(ReportDefinition.objects.only("source_id"), pk=pk)
            source_ids = [report.source_id]
            new_source_id = _posted_source_id(request) if may_change_source else None
            if new_source_id is not None:
                source_ids.append(new_source_id)
            return _guarded_report_write(request, view, pk=pk, source_ids=source_ids)
        return wrapped
    return decorate
