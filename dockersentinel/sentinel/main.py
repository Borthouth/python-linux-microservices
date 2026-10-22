from fastapi import FastAPI, BackgroundTasks, Query
from .config import settings
from .docker_monitor import DockerSentinel
from .host_monitor import HostSentinel
from .notifier import dispatch_alerts_if_needed

app = FastAPI(title=settings.APP_NAME, version="0.1.0")
docker_sentinel = DockerSentinel()


@app.get("/health")
def health_check():
    return {"status": "ok", "app": settings.APP_NAME}


@app.get("/api/v1/sentinel/summary")
def get_sentinel_summary(
    background_tasks: BackgroundTasks,
    name: str | None = Query(default=None, description="Filter containers by name (partial match)")
):
    containers = docker_sentinel.get_container_states(name_filter=name)
    host_metrics = HostSentinel.get_system_metrics()

    flagged_containers = [c for c in containers if c["flagged"]]
    has_host_alerts = host_metrics["ram"]["high_usage"] or host_metrics["disk"]["high_usage"]
    
    system_status = "CRITICAL" if (flagged_containers or has_host_alerts) else "HEALTHY"

    response = {
        "overall_status": system_status,
        "host_metrics": host_metrics,
        "containers": {
            "total_count": len(containers),
            "flagged_count": len(flagged_containers),
            "details": containers
        }
    }

    if system_status == "CRITICAL" and settings.WEBHOOK_URL:
        background_tasks.add_task(dispatch_alerts_if_needed, response)

    return response