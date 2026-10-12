"""Execute real DAG runs and compare PostgreSQL state. Synthetic data only."""
import json
import sys
from bootstrap import configure

configure()
sys.path.insert(0, '/opt/airflow/dags')
from transaction_reporting import transaction_reporting_dag as dag, make_transactions
from airflow.providers.postgres.hooks.postgres import PostgresHook


def snapshot():
    hook = PostgresHook(postgres_conn_id='payments_db')
    return {
        'raw': [list(map(str, row)) for row in hook.get_records('SELECT * FROM payment_demo.raw_transactions ORDER BY tx_id')],
        'report': [list(map(str, row)) for row in hook.get_records('SELECT * FROM payment_demo.daily_transaction_report ORDER BY report_date,currency')],
    }


def run(conf, expected):
    result = dag.test(run_conf=conf)
    state = str(result.state)
    if state != expected:
        raise AssertionError(f'Expected DAG state {expected}, got {state}')
    return state


conf = {'report_date': '2026-10-01', 'transactions': None}
results = {'data_kind': 'synthetic payment data', 'baseline': run(conf, 'success')}
first = snapshot()
assert len(first['raw']) == 200, 'Use a fresh dedicated teaching database'
assert len(first['report']) == 3
results['replay'] = run(conf, 'success')
assert snapshot() == first, 'Replay changed persisted rows'
invalid = make_transactions('2026-10-02')
invalid[0]['amount_cents'] = -100
results['invalid_input'] = run({'report_date': '2026-10-02', 'transactions': invalid}, 'failed')
assert snapshot() == first, 'Rejected input changed persisted rows'
results.update(raw_rows=200, currency_groups=3, replay_unchanged=True, failure_isolated=True)
print('INTEGRATION_RESULT=' + json.dumps(results, sort_keys=True))
