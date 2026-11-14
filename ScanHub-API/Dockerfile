FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SCANHUB_TEMP_DIR=/var/lib/scanhub/tmp

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --system --uid 10001 --no-create-home scanhub \
    && mkdir -p /app/logs /var/lib/scanhub/tmp \
    && chown -R scanhub:scanhub /app/logs /var/lib/scanhub

COPY --chown=root:root . .

USER scanhub

EXPOSE 9443

CMD ["hypercorn", "--config", "hypercorn-https.toml", "--worker-class", "uvloop", "scan_api:app"]
