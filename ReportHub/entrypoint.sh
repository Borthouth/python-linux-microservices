#!/bin/sh

# Wait for PostgreSQL to start (using the correct service name 'postgres')
echo "Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  sleep 0.1
done
echo "PostgreSQL started successfully."

# Run Django migrations automatically
python manage.py migrate

# Start the Django development server, binding to all network interfaces
exec python manage.py runserver 0.0.0.0:8000