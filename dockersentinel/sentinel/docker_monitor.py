import logging
import docker
from .config import settings

logger = logging.getLogger(__name__)


class DockerSentinel:
    def __init__(self):
        try:
            self.client = docker.DockerClient(base_url=settings.DOCKER_SOCKET_PATH)
        except Exception as exc:
            logger.error(f"Failed to connect to Docker socket at {settings.DOCKER_SOCKET_PATH}: {exc}")
            self.client = None

    def get_container_states(self, name_filter: str | None = None) -> list[dict]:
        if not self.client:
            return []

        containers_summary = []
        try:
            for container in self.client.containers.list(all=True):
                # Skip if a name filter is provided and doesn't match the container name
                if name_filter and name_filter.lower() not in container.name.lower():
                    continue

                attrs = container.attrs
                state = attrs.get("State", {})
                
                health_status = state.get("Health", {}).get("Status", "no_healthcheck")
                restart_count = attrs.get("RestartCount", 0)

                status_info = {
                    "id": container.short_id,
                    "name": container.name,
                    "status": container.status,
                    "health": health_status,
                    "restart_count": restart_count,
                    "image": container.image.tags[0] if container.image.tags else "unknown",
                    "flagged": False,
                    "reasons": []
                }

                if restart_count >= settings.CONTAINER_RESTART_THRESHOLD:
                    status_info["flagged"] = True
                    status_info["reasons"].append(f"Excessive restarts ({restart_count})")

                if health_status == "unhealthy":
                    status_info["flagged"] = True
                    status_info["reasons"].append("Healthcheck status is UNHEALTHY")

                if container.status == "exited" and state.get("ExitCode", 0) != 0:
                    status_info["flagged"] = True
                    status_info["reasons"].append(f"Container exited abnormally with code {state.get('ExitCode')}")

                containers_summary.append(status_info)

        except Exception as exc:
            logger.error(f"Error querying Docker API: {exc}")

        return containers_summary