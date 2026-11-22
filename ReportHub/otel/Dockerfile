ARG OTELCOL_VERSION=0.165.0
FROM otel/opentelemetry-collector-contrib:${OTELCOL_VERSION} AS collector

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends systemd python3 python3-yaml \
    && rm -rf /var/lib/apt/lists/*

COPY --from=collector /otelcol-contrib /usr/local/bin/otelcol-contrib
COPY supervise.py source_policy.py /opt/reporthub-otel/

USER 0:0
ENTRYPOINT ["python3", "/opt/reporthub-otel/supervise.py"]
