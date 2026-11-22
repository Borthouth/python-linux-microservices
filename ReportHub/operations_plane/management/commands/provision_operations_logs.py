import time

import clickhouse_connect
from clickhouse_connect.driver.exceptions import ClickHouseError
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = 'Create the ClickHouse database and OpenTelemetry table used for Operations logs.'

    def handle(self, *args, **options):
        for attempt in range(10):
            client = None
            try:
                client = clickhouse_connect.get_client(
                    host=settings.CLICKHOUSE_HOST,
                    port=settings.CLICKHOUSE_PORT,
                    username=settings.OPERATIONS_LOGS_CLICKHOUSE_USER,
                    password=settings.OPERATIONS_LOGS_CLICKHOUSE_PASSWORD,
                    connect_timeout=3,
                    send_receive_timeout=10,
                )
                client.command('CREATE DATABASE IF NOT EXISTS operations')
                client.command(
                    '''
                    CREATE TABLE IF NOT EXISTS operations.otel_logs (
                        event_id UUID DEFAULT generateUUIDv4(),
                        ingested_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
                        Timestamp DateTime64(9, 'UTC'),
                        TraceId String,
                        SpanId String,
                        TraceFlags UInt8,
                        SeverityText LowCardinality(String),
                        SeverityNumber UInt8,
                        ServiceName LowCardinality(String),
                        Body String,
                        ResourceSchemaUrl String,
                        ResourceAttributes Map(String, String),
                        ScopeSchemaUrl String,
                        ScopeName String,
                        ScopeVersion String,
                        ScopeAttributes Map(String, String),
                        LogAttributes Map(String, String),
                        EventName String
                    )
                    ENGINE = MergeTree
                    PARTITION BY toYYYYMM(Timestamp)
                    ORDER BY (Timestamp, event_id)
                    '''
                )
                break
            except (ClickHouseError, OSError, TimeoutError) as exc:
                if attempt == 9:
                    raise CommandError(
                        f'Could not provision Operations logs in ClickHouse ({type(exc).__name__}).'
                    ) from exc
                time.sleep(2)
            finally:
                if client is not None:
                    client.close()

        self.stdout.write(self.style.SUCCESS('Operations OpenTelemetry log storage is ready.'))
