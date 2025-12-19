#!/usr/bin/env bash

set -e

echo 'Started Postgres init...'

createdb vault_db
createdb rate_service_db

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "vault_db" <<-EOSQL
	GRANT ALL PRIVILEGES ON DATABASE vault_db TO $POSTGRES_USER;
	GRANT ALL PRIVILEGES ON DATABASE rate_service_db TO $POSTGRES_USER;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "vault_db" -a -f /schema/vault-schema.sql

echo 'Completed Postgres init'