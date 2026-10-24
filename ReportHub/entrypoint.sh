#!/bin/sh

# Wait for PostgreSQL port to open
echo "Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  sleep 0.1
done

# Give PostgreSQL a short grace period to finish initialization/recovery
sleep 2
echo "PostgreSQL started successfully."

# Run Django migrations automatically
python manage.py migrate

# Execute the container's main command
exec "$@"