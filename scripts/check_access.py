"""Verify database privilege separation using actual connections."""
import json
from pathlib import Path
import psycopg2

for role, forbidden in [('payments', 'airflow'), ('airflow', 'payments')]:
    password = Path('/run/secrets', role + '_password').read_text().strip()
    with psycopg2.connect(host='postgres', user=role, password=password, dbname=role, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = current_user')
            if cur.fetchone() != (False, False, False):
                raise AssertionError(f'{role} has excessive privileges')
    try:
        conn = psycopg2.connect(host='postgres', user=role, password=password, dbname=forbidden, connect_timeout=5)
    except psycopg2.OperationalError as exc:
        if 'permission denied for database' not in str(exc):
            raise
    else:
        conn.close()
        raise AssertionError(f'{role} unexpectedly accessed {forbidden}')
print(json.dumps({'role_separation': 'passed', 'roles': ['payments', 'airflow']}))
