FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY sentinel/ ./sentinel/
COPY certs/ ./certs/

EXPOSE 8000

CMD ["uvicorn", "sentinel.main:app", "--host", "0.0.0.0", "--port", "8000", "--ssl-keyfile=certs/key.pem", "--ssl-certfile=certs/cert.pem"]