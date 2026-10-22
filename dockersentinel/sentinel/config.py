from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_NAME: str = "DockerSentinel Resource & Container Watch"
    DEBUG: bool = False
    
    # Thresholds
    DISK_THRESHOLD_PERCENT: float = 85.0
    RAM_THRESHOLD_PERCENT: float = 90.0
    CONTAINER_RESTART_THRESHOLD: int = 3
    
    # Docker Socket Location
    DOCKER_SOCKET_PATH: str = "unix://var/run/docker.sock"
    
    # Notification Settings
    WEBHOOK_URL: str | None = None  # Internal webhook target (Slack, Discord, Teams)
    
    class Config:
        env_file = ".env"


settings = Settings()