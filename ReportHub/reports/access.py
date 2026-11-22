from control_plane.models import ControlPlaneUser
from reports.models import ReportProfileRecipient, ReportUserRecipient


def report_access_start(report, account):
    if not account or not account.is_active or account.archived_at or not account.auth_user.is_active:
        return None

    starts = list(ReportUserRecipient.objects.filter(
        report=report, user=account, created_at__isnull=False,
    ).values_list("created_at", flat=True))
    memberships = ReportProfileRecipient.objects.filter(
        report=report,
        profile__is_active=True,
        profile__memberships__user=account,
        created_at__isnull=False,
        profile__memberships__created_at__isnull=False,
    ).values_list("created_at", "profile__memberships__created_at")
    starts.extend(max(grant, membership) for grant, membership in memberships)
    return min(starts) if starts else None


def report_recipient_accounts(report):
    user_ids = set(report.user_recipients.values_list("user_id", flat=True))
    user_ids.update(
        report.profile_recipients.values_list("profile__memberships__user_id", flat=True)
    )
    user_ids.discard(None)
    return ControlPlaneUser.objects.filter(pk__in=user_ids).select_related("auth_user")
