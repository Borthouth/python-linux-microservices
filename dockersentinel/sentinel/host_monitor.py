import subprocess
import psutil
from .config import settings


class HostSentinel:
    @staticmethod
    def get_system_metrics() -> dict:
        virtual_mem = psutil.virtual_memory()
        disk_usage = psutil.disk_usage("/")

        metrics = {
            "ram": {
                "total_gb": round(virtual_mem.total / (1024 ** 3), 2),
                "used_gb": round(virtual_mem.used / (1024 ** 3), 2),
                "percent_used": virtual_mem.percent,
                "high_usage": virtual_mem.percent >= settings.RAM_THRESHOLD_PERCENT
            },
            "disk": {
                "total_gb": round(disk_usage.total / (1024 ** 3), 2),
                "used_gb": round(disk_usage.used / (1024 ** 3), 2),
                "free_gb": round(disk_usage.free / (1024 ** 3), 2),
                "percent_used": disk_usage.percent,
                "high_usage": disk_usage.percent >= settings.DISK_THRESHOLD_PERCENT
            }
        }
        return metrics

    @staticmethod
    def check_systemd_service(service_name: str) -> dict:
        try:
            result = subprocess.run(
                ["systemctl", "is-active", service_name],
                capture_output=True,
                text=True,
                timeout=3
            )
            status = result.stdout.strip()
            return {"service": service_name, "active": status == "active", "state": status}
        except Exception as exc:
            return {"service": service_name, "active": False, "state": f"error: {str(exc)}"}