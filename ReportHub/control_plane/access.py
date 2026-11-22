from control_plane.models import (
    AnnouncementPermission,
    ControlPlaneProfilePermission,
    ControlPlaneProfileSourceAccess,
    ControlPlaneUser,
    Role,
    UserSourceAccess,
)


_ROLE_RANK = {
    Role.VIEWER: 0,
    Role.ANALYST: 1,
    Role.ADMIN: 2,
}


def effective_source_roles(user):
    if not getattr(user, "is_authenticated", False) or not user.is_active:
        return {}

    account = ControlPlaneUser.objects.filter(auth_user_id=user.pk).first()
    if account and (not account.is_active or account.archived_at):
        return {}

    roles = {}
    direct_access = UserSourceAccess.objects.filter(user=user).values_list(
        "source_id", "role"
    )
    for source_id, role in direct_access:
        roles[source_id] = role

    if account:
        profile_access = ControlPlaneProfileSourceAccess.objects.filter(
            profile__memberships__user=account,
            profile__is_active=True,
        ).values_list("source_id", "role")
        for source_id, role in profile_access:
            current_role = roles.get(source_id)
            if current_role is None or _ROLE_RANK[role] > _ROLE_RANK[current_role]:
                roles[source_id] = role

    return roles


def authorized_source_ids(user, role=None):
    source_roles = effective_source_roles(user)
    if role is None:
        return list(source_roles)
    return [
        source_id
        for source_id, effective_role in source_roles.items()
        if _ROLE_RANK[effective_role] >= _ROLE_RANK[role]
    ]


def has_source_access(user):
    return bool(effective_source_roles(user))


def effective_announcement_permissions(user):
    if not getattr(user, "is_authenticated", False) or not user.is_active:
        return set()

    if hasattr(user, "operations_account"):
        return set()

    return set(
        ControlPlaneProfilePermission.objects.filter(
            profile__memberships__user__auth_user_id=user.pk,
            profile__memberships__user__is_active=True,
            profile__memberships__user__archived_at__isnull=True,
            profile__is_active=True,
            permission__code__in=AnnouncementPermission.values,
            permission__is_active=True,
        ).values_list("permission_id", flat=True)
    )


def has_announcement_permission(user, permission):
    permissions = effective_announcement_permissions(user)
    if permission == AnnouncementPermission.PUBLISH_CROSS_PLANE:
        return (
            AnnouncementPermission.PUBLISH in permissions
            and permission in permissions
        )
    return permission in permissions
