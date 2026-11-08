#!/bin/sh
set -eu

key_file=/data/configdb/replica-set.key
if [ ! -s "$key_file" ]; then
    umask 077
    openssl rand -base64 756 > "$key_file"
fi
chmod 400 "$key_file"
chown mongodb:mongodb "$key_file"

exec /usr/local/bin/docker-entrypoint.sh \
    mongod --replSet rs0 --bind_ip_all --keyFile "$key_file"
