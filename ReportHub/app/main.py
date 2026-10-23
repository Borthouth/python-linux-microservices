from fastapi import FastAPI

app = FastAPI(
    title="ReportHub Microservice",
    version="1.0.0",
    description="Microservice for report generation and processing"
)

@app.get("/")
def read_root():
    return {"service": "ReportHub", "status": "running"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}