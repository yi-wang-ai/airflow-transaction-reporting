#!/bin/bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
  --set=payments_password="$(cat /run/secrets/payments_password)" \
  --set=airflow_password="$(cat /run/secrets/airflow_password)" <<'SQL'
CREATE ROLE payments LOGIN PASSWORD :'payments_password' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE ROLE airflow LOGIN PASSWORD :'airflow_password' NOSUPERUSER NOCREATEDB NOCREATEROLE;
CREATE DATABASE payments OWNER payments;
CREATE DATABASE airflow OWNER airflow;
REVOKE CONNECT ON DATABASE payments FROM PUBLIC;
REVOKE CONNECT ON DATABASE airflow FROM PUBLIC;
GRANT CONNECT ON DATABASE payments TO payments;
GRANT CONNECT ON DATABASE airflow TO airflow;
SQL
