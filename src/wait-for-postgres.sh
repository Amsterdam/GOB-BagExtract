#!/bin/bash
# wait-for-postgres.sh

set -e

host="$1"
shift

max=60
current=0

if ! PGPASSWORD=$DATABASE_PASSWORD psql -h "$host" -U "$DATABASE_USER" -c '\q'; then

  >&2 echo "Wait 10 seconds for Postgres to come alive"
  sleep 10

  until PGPASSWORD=$DATABASE_PASSWORD psql -h "$host" -U "$DATABASE_USER" -c '\q'; do
    >&2 echo "Postgres is unavailable - sleeping"
    sleep 1

    if [ $((current++)) -ge $max ]; then
      >&2 echo "Timed out waiting for Postgres"
      exit 1
    fi
  done

fi

>&2 echo -e "\nPostgres is up - executing command '$@'"
exec "$@"
