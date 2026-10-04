#!/bin/sh
set -eu

# postgres' entrypoint runs this only when the data directory is first created.
# Apply forward migrations only; never execute *.down.sql.
for file in /migrations/0001_initial.sql /migrations/0002_schema_lockdown.sql /migrations/0003_outbox.sql /migrations/0004_analytics.sql /migrations/*.up.sql /migrations/9999_grant_app_roles.sql; do
  [ -f "$file" ] || continue
  echo "applying $file"
  psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --file "$file"
done
