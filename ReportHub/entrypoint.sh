#!/bin/sh

case "${MIGRATIONS_ON_STARTUP-1}" in
  0|1) ;;
  *)
    echo "MIGRATIONS_ON_STARTUP must be 0 or 1." >&2
    exit 2
    ;;
esac

# Wait for PostgreSQL port to open
echo "Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  sleep 0.1
done

# Give PostgreSQL a short grace period to finish initialization/recovery
sleep 2
echo "PostgreSQL started successfully."

# Run Django migrations automatically
if [ "${MIGRATIONS_ON_STARTUP-1}" = "1" ]; then
  python manage.py migrate || {
    status=$?
    echo "Django migrations failed; refusing to start the main command." >&2
    exit "$status"
  }
fi

# Execute the container's main command
exec "$@"