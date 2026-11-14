"""Helpers used only by the disposable migration-gate Compose test."""

import os
import sys
import time

import psycopg2


def admin_connection():
    return psycopg2.connect(
        host="postgres", dbname="reporthub", user="gate_admin",
        password=os.environ["GATE_ADMIN_PASSWORD"],
    )


def provision():
    connection = admin_connection()
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            for role, password in (
                ("gate_migrator", os.environ["GATE_MIGRATION_PASSWORD"]),
                ("gate_runtime", os.environ["GATE_RUNTIME_PASSWORD"]),
            ):
                cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
                if cursor.fetchone() is None:
                    cursor.execute(
                        f"CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB "
                        "NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD %s",
                        (password,),
                    )
            cursor.execute("ALTER DATABASE reporthub OWNER TO gate_migrator")
            cursor.execute("REVOKE ALL ON DATABASE reporthub FROM PUBLIC")
            cursor.execute("GRANT CONNECT ON DATABASE reporthub TO gate_runtime")
            cursor.execute("ALTER SCHEMA public OWNER TO gate_migrator")
            cursor.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC")
            cursor.execute("GRANT USAGE ON SCHEMA public TO gate_runtime")
            cursor.execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE gate_migrator IN SCHEMA public "
                "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO gate_runtime"
            )
            cursor.execute(
                "ALTER DEFAULT PRIVILEGES FOR ROLE gate_migrator IN SCHEMA public "
                "GRANT USAGE, SELECT ON SEQUENCES TO gate_runtime"
            )
            cursor.execute(
                """
                CREATE OR REPLACE FUNCTION public.gate_block_migration_ddl()
                RETURNS event_trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF current_user = 'gate_migrator' THEN
                        RAISE EXCEPTION 'gate blocks migration DDL';
                    END IF;
                END
                $$
                """
            )
            cursor.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_event_trigger
                        WHERE evtname = 'gate_block_migration_ddl'
                    ) THEN
                        CREATE EVENT TRIGGER gate_block_migration_ddl
                        ON ddl_command_start
                        EXECUTE FUNCTION public.gate_block_migration_ddl();
                    END IF;
                END
                $$
                """
            )
    finally:
        connection.close()


def check_runtime():
    import django

    django.setup()
    from django.db import connection
    from control_plane.models import SourceProfile

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT current_user, has_schema_privilege(current_user, 'public', 'CREATE'), "
            "has_table_privilege(current_user, 'control_plane_sourceprofile', 'TRUNCATE')"
        )
        if cursor.fetchone() != ("gate_runtime", False, False):
            raise AssertionError("Runtime identity or denied privileges are incorrect.")
        cursor.execute(
            "SELECT count(*) FROM django_migrations "
            "WHERE app = 'control_plane' "
            "AND name = '0041_alter_sourceprofile_last_sync_status'"
        )
        if cursor.fetchone() != (1,):
            raise AssertionError("The required migration has not run.")
    source = SourceProfile.objects.create(
        name="migration-gate-probe", host="unused", username="unused",
        database_name="unused",
    )
    source.delete()
    print("GATE_RUNTIME_VERIFIED", flush=True)


if __name__ == "__main__":
    if sys.argv[1] == "provision":
        provision()
    elif sys.argv[1] == "runtime":
        check_runtime()
        while True:
            time.sleep(60)
    elif sys.argv[1] == "check":
        check_runtime()
    else:
        raise ValueError(f"Unknown migration gate probe mode: {sys.argv[1]}")
