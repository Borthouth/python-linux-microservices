from enum import IntEnum


class ReportProgress(IntEnum):
    INITIALIZING = 10
    QUERYING = 30
    FORMATTING = 60
    UPLOADING = 85
    COMPLETED = 100


REPORT_RUNNING_STEPS = (
    ("initializing", "Initializing report execution", ReportProgress.INITIALIZING),
    ("querying", "Querying ClickHouse", ReportProgress.QUERYING),
    ("formatting", "Building summaries and formatting file", ReportProgress.FORMATTING),
    ("uploading", "Uploading artifact to MinIO", ReportProgress.UPLOADING),
)
