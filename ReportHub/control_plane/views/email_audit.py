from django.contrib.admin.views.decorators import staff_member_required
from django.core.paginator import Paginator
from django.db.models import Q
from django.shortcuts import render, get_object_or_404, redirect
from django.views.decorators.csrf import csrf_exempt
from reports.models import EmailAuditLog
from reports.email_webhooks import handle_email_webhook
from django.contrib import messages


@staff_member_required
def email_audit_log_view(request):
    """
    Control Plane governance view for monitoring email dispatches,
    delivery statuses, failure logs, and bounce metrics with search & pagination.
    """
    status_filter = request.GET.get('status', '')
    search_query = request.GET.get('q', '').strip()

    # Fixed: using '-created_at' instead of invalid '-random'
    logs = EmailAuditLog.objects.select_related('report', 'execution').order_by('-created_at')

    if status_filter:
        logs = logs.filter(status=status_filter)

    if search_query:
        logs = logs.filter(
            Q(recipients__icontains=search_query) |
            Q(report__name__icontains=search_query) |
            Q(error_message__icontains=search_query)
        )

    # Paginate results (20 items per page)
    paginator = Paginator(logs, 20)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
        'logs': page_obj,  
        'current_status': status_filter,
        'status_filter': status_filter,
        'search_query': search_query,
        'filter_options': [
            {'value': 'sent', 'label': 'Sent'},
            {'value': 'delivered', 'label': 'Delivered'},
            {'value': 'failed', 'label': 'Failed'},
            {'value': 'bounced', 'label': 'Bounced'},
        ],
    }
    return render(request, 'control_plane/email_audit.html', context)


@csrf_exempt
def email_webhook_receiver(request):
    """
    Public webhook endpoint for handling external SMTP provider delivery feedback,
    bounces, drops, and delivered events.
    """
    return handle_email_webhook(request, extended_events=True)


@staff_member_required
def email_audit_requeue_view(request, log_id):
    log = get_object_or_404(EmailAuditLog, pk=log_id)
    if request.method == 'POST':
        log.status = 'sent'
        log.retry_count = getattr(log, 'retry_count', 0) + 1
        log.error_message = '' # Clear previous error on retry
        log.save()
        messages.success(request, f"Successfully re-queued email dispatch for {log.recipients}.")
    return redirect('control_plane:email_audit_logs')


@staff_member_required
def email_audit_delete_view(request, log_id):
    log = get_object_or_404(EmailAuditLog, pk=log_id)
    if request.method == 'POST':
        recipient = log.recipients
        log.delete()
        messages.success(request, f"Successfully deleted audit record for {recipient}.")
    return redirect('control_plane:email_audit_logs')