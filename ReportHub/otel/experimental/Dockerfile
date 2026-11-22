FROM otel/opentelemetry-collector-builder:0.165.0 AS builder
USER root
WORKDIR /build
ENV CGO_ENABLED=0 GOMAXPROCS=2 GOFLAGS=-p=2
COPY builder.yaml /build/builder.yaml
RUN ocb --config=/build/builder.yaml

FROM scratch
COPY --from=builder /build/output/otelcol-reassembly-probe /otelcol-reassembly-probe
USER 10001:10001
ENTRYPOINT ["/otelcol-reassembly-probe"]
