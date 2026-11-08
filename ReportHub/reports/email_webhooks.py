import json
import logging

from django.http import JsonResponse

from .models import EmailAuditLog


logger = logging.getLogger(__name__)


def handle_email_webhook(request, *, extended_events=False):
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    try:
        events = json.loads(request.body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JsonResponse({"error": "Invalid webhook payload."}, status=400)

    if extended_events and isinstance(events, dict):
        events = [events]
    if not isinstance(events, list) or not all(isinstance(event, dict) for event in events):
        return JsonResponse({"error": "Invalid webhook payload."}, status=400)

    bounce_types = {"bounce", "dropped"}
    delivered_types = {"delivered"}
    if extended_events:
        bounce_types.add("Bounce")
        delivered_types.add("Delivered")

    try:
        for event in events:
            msg_id = event.get("sg_message_id") or event.get("message-id")
            event_type = event.get("event")
            if extended_events:
                msg_id = msg_id or event.get("MessageID")
                event_type = event_type or event.get("Type")

            if msg_id:
                log_entry = EmailAuditLog.objects.filter(message_id__icontains=msg_id).first()
                if log_entry:
                    if event_type in bounce_types:
                        log_entry.status = "bounced"
                        log_entry.error_message = event.get("reason", "Unknown bounce reason")
                    elif event_type in delivered_types:
                        log_entry.status = "delivered"
                    log_entry.save()
    except Exception:
        logger.exception("Failed to process email delivery webhook")
        return JsonResponse({"error": "Failed to process webhook."}, status=500)

    return JsonResponse({"status": "success"}, status=200)
