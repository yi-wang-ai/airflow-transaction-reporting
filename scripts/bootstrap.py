"""Inject file-backed credentials, then execute Airflow without a shell."""
import json
import os
from pathlib import Path
import sys
from urllib.parse import quote


def configure():
    secret = lambda name: Path('/run/secrets', name).read_text().strip()
    os.environ['AIRFLOW_CONN_PAYMENTS_DB'] = json.dumps({
        'conn_type': 'postgres', 'host': 'postgres', 'port': 5432,
        'schema': 'payments', 'login': 'payments', 'password': secret('payments_password'),
    })
    os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'] = (
        'postgresql+psycopg2://airflow:' + quote(secret('airflow_password'), safe='') + '@postgres/airflow'
    )
    os.environ['AIRFLOW__CORE__FERNET_KEY'] = secret('fernet_key')
    os.environ['AIRFLOW__API_AUTH__JWT_SECRET'] = secret('jwt_secret')


if __name__ == '__main__':
    configure()
    os.execvp('airflow', ['airflow', *sys.argv[1:]])
