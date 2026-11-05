import redis
from django.conf import settings
from django.utils import timezone

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