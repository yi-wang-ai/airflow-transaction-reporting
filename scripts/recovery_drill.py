"""Back up payments and verify a restore into a NEW isolated database.

Never overwrites the source database. Retains dump, checksums and comparison.
This tests logical recovery on the same PostgreSQL server, not host disaster recovery.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
BASE = ['docker', 'compose', '-f', str(ROOT / 'deployment/compose.yaml'), 'exec', '-T', 'postgres']
SQL = """
SELECT json_build_object(
 'raw_count', (SELECT count(*) FROM payment_demo.raw_transactions),
 'raw_digest', (SELECT md5(COALESCE(string_agg(row_to_json(t)::text, E'\\n' ORDER BY tx_id),'')) FROM payment_demo.raw_transactions t),
 'report_count', (SELECT count(*) FROM payment_demo.daily_transaction_report),
 'report_digest', (SELECT md5(COALESCE(string_agg(row_to_json(t)::text, E'\\n' ORDER BY report_date,currency),'')) FROM payment_demo.daily_transaction_report t)
);
"""


def command(args, **kwargs):
    return subprocess.run(BASE + args, check=True, **kwargs)


def fingerprint(database):
    return json.loads(command(['psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database,
                               '-At', '-c', SQL], stdout=subprocess.PIPE).stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new evidence directory')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    dump = args.output / 'payments.dump'
    restored = 'restore_check_' + uuid.uuid4().hex
    record = {'started_utc': datetime.now(timezone.utc).isoformat(), 'data_kind': 'synthetic',
              'scope': 'logical restore into isolated database on same server', 'status': 'failed'}
    created = False
    started = time.monotonic()
    try:
        record['source_before'] = fingerprint('payments')
        if record['source_before']['raw_count'] == 0:
            raise RuntimeError('Refusing to claim recovery from an empty dataset; run integration checks first')
        with dump.open('xb') as target:
            command(['pg_dump', '-U', 'postgres', '-d', 'payments', '-Fc', '--no-owner', '--no-acl'], stdout=target)
        record['dump_sha256'] = hashlib.sha256(dump.read_bytes()).hexdigest()
        command(['createdb', '-U', 'postgres', restored])
        created = True
        restore_started = time.monotonic()
        with dump.open('rb') as source:
            command(['pg_restore', '-U', 'postgres', '-d', restored, '--exit-on-error', '--no-owner', '--no-acl'], stdin=source)
        record['restore_seconds'] = round(time.monotonic() - restore_started, 3)
        record['restored'] = fingerprint(restored)
        record['source_after'] = fingerprint('payments')
        if not record['source_before'] == record['restored'] == record['source_after']:
            raise RuntimeError('Restore mismatch or source changed during drill')
        record['status'] = 'passed'
    except Exception as exc:
        record['error'] = str(exc)
        raise
    finally:
        record['elapsed_seconds'] = round(time.monotonic() - started, 3)
        (args.output / 'recovery.json').write_text(json.dumps(record, indent=2) + '\n')
        if created:
            command(['dropdb', '-U', 'postgres', restored])
    print(json.dumps(record, indent=2))


if __name__ == '__main__':
    main()
