cat << 'EOF' > entrypoint.sh
#!/bin/sh

# Wait for PostgreSQL to start
echo "Waiting for PostgreSQL..."
while ! nc -z db 5432; do
  sleep 0.1
done
echo "PostgreSQL started successfully."

# Run Django migrations automatically
python manage.py migrate

# Execute container command
exec "$@"
EOF