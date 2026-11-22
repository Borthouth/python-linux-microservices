import redis
from uuid import UUID, uuid4

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db.models.functions import Lower, Trim
from django.utils import timezone
from django.utils.text import slugify


USER_DETAILS_ERROR = "Unable to save user. Check the details or contact an administrator."


def generate_control_plane_username(first_name, last_name):
    name = slugify(f"{first_name}-{last_name}") or "user"
    return f"{name[:110]}-{uuid4().hex[:8]}"


def validate_control_plane_email(email, exclude_id=None):
    from control_plane.models import ControlPlaneUser

    accounts = ControlPlaneUser.objects.filter(archived_at__isnull=True).annotate(
        normalized_email=Lower(Trim("email")),
    ).filter(normalized_email__iexact=email)
    if exclude_id is not None:
        accounts = accounts.exclude(pk=exclude_id)
    if accounts.exists():
        raise ValidationError(USER_DETAILS_ERROR)
    return email


def add_email_conflict_error(form, exclude_id=None):
    try:
        validate_control_plane_email(form.cleaned_data["email"], exclude_id)
    except ValidationError as error:
        form.add_error("email", error)
        return True
    return False


def user_identity(user):
    account = getattr(user, "control_plane_account", None) or getattr(
        user, "operations_account", None
    )
    if account:
        username = account.username
        name = f"{account.first_name} {account.last_name}".strip() or username
    else:
        username = user.username
        try:
            UUID(username)
        except ValueError:
            pass
        else:
            username = ""
        name = user.get_full_name().strip() or username or "User"

    label = name
    if username and username != name:
        label += f" ({username})"
    label += f" [User ID: {user.pk}]"
    return {"name": name, "username": username, "label": label}


def get_redis_client():
    """Returns a Redis client instance connected to DB 1 (Task Telemetry)."""
    # Fallback to the docker service name 'redis' instead of 'localhost'
    redis_url = getattr(settings, 'REDIS_URL', 'redis://redis:6379/1')
    
    if not redis_url.endswith('/1'):
        base_url = redis_url.rsplit('/', 1)[0]
        redis_url = f"{base_url}/1"
        
    return redis.Redis.from_url(redis_url)

def set_task_progress(task_id, percentage, status="RUNNING", **kwargs):
    """
    Stores ephemeral task progress and telemetry metadata into Redis Hash (DB 1)
    to eliminate PostgreSQL write contention.
    """
    r = get_redis_client()
    key = f"task:progress:{task_id}"
    
    mapping = {
        "percentage": str(percentage),
        "status": str(status),
    }

    if status == "RUNNING":
        r.hsetnx(key, "started_at", timezone.now().isoformat())
    
    # Add any extra metadata (current_table, source_name, error, etc.)
    for k, v in kwargs.items():
        if v is not None:
            mapping[k] = str(v)
            
    r.hset(key, mapping=mapping)
    # Set an expiry on the progress key (e.g., 24 hours) to prevent Redis memory bloat
    r.expire(key, 86400)

def get_task_progress(task_id):
    """Retrieves task progress dictionary from Redis DB 1."""
    r = get_redis_client()
    key = f"task:progress:{task_id}"
    data = r.hgetall(key)
    if not data:
        return {"percentage": 0, "status": "UNKNOWN"}
        
    return {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}