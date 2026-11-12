from contextlib import ExitStack

from django.contrib import messages
from django.db import transaction
from django.shortcuts import redirect

from control_plane.metadata_guard import SourceMetadataCutoverPending, source_metadata_write_guard
from control_plane.models import SourceProfile


class SourceMetadataAdminMixin:
    def _source_ids(self, objects):
        if self.model is SourceProfile:
            return {obj.pk for obj in objects}
        return {obj.source_id for obj in objects}

    def _guarded_admin_action(self, request, objects, action, *, include_posted_source=False):
        source_ids = self._source_ids(objects)
        if include_posted_source:
            value = request.POST.get("source", "")
            try:
                posted_source = int(value)
            except (TypeError, ValueError):
                posted_source = None
            if posted_source is not None and 0 < posted_source < 2**63:
                source_ids.add(posted_source)
        try:
            with ExitStack() as stack:
                stack.enter_context(transaction.atomic())
                for source_id in sorted(source_ids):
                    stack.enter_context(source_metadata_write_guard(source_id))
                locked = list(
                    self.get_queryset(request).select_for_update().filter(
                        pk__in=[obj.pk for obj in objects]
                    )
                )
                if self._source_ids(locked) - source_ids:
                    messages.error(request, "The source changed; reload and try again.")
                    return redirect(request.path)
                return action()
        except SourceMetadataCutoverPending as exc:
            messages.error(request, str(exc))
            return redirect(request.path)

    def changeform_view(self, request, object_id=None, form_url="", extra_context=None):
        action = lambda: super(SourceMetadataAdminMixin, self).changeform_view(
            request, object_id, form_url, extra_context
        )
        if request.method != "POST":
            return action()
        obj = self.get_object(request, object_id) if object_id is not None else None
        if obj is None and (object_id is not None or self.model is SourceProfile):
            return action()
        return self._guarded_admin_action(
            request, [obj] if obj else [], action,
            include_posted_source=self.model is not SourceProfile,
        )

    def delete_view(self, request, object_id, extra_context=None):
        action = lambda: super(SourceMetadataAdminMixin, self).delete_view(
            request, object_id, extra_context
        )
        if request.method != "POST":
            return action()
        obj = self.get_object(request, object_id)
        if obj is None:
            return action()
        return self._guarded_admin_action(request, [obj], action)

    def changelist_view(self, request, extra_context=None):
        action = lambda: super(SourceMetadataAdminMixin, self).changelist_view(
            request, extra_context
        )
        if request.method != "POST" or not request.POST.get("action"):
            return action()
        if request.POST.get("select_across") == "1":
            messages.error(request, "Select the source records explicitly for this action.")
            return redirect(request.path)
        selected = request.POST.getlist("_selected_action")
        if not selected or any(not item.isdecimal() for item in selected):
            return action()
        objects = list(self.get_queryset(request).filter(pk__in=selected))
        return self._guarded_admin_action(request, objects, action)
