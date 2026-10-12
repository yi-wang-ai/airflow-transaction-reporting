# Standalone deployment validation — 12 October 2026

Executed locally on Docker Desktop, Linux ARM64 containers on a macOS ARM64 host.
Airflow 3.1.0 / Python 3.12 and PostgreSQL 14.24; exact image digests are pinned
in deployment/compose.yaml. These are local results, not GitHub workflow results.

| Check | Observed result |
|---|---|
| Clean-volume deployment | PostgreSQL and Airflow became healthy |
| Existing pure-function tests | 22 passed |
| Real Airflow DAG baseline | Success; 200 transactions, three currency reports |
| Replay | Success; complete database snapshots unchanged |
| Invalid negative amount | Validation failed as expected; database unchanged |
| Database privilege separation | Both roles non-superuser; cross-database access rejected |
| Backup and isolated restore | Passed; row counts and full-table fingerprints identical |
| Source preservation | Original database fingerprint unchanged after restore |
| Controlled database outage | Health probe failed during outage; recovered after restart |
| Secret scan | Generated secret values absent from retained evidence |

The fresh deployment was tested after correcting the init-script interpreter and
Airflow state-volume ownership. The final configuration boots from fresh volumes;
manual repair is not part of the final startup instructions.

See integration-result.json, integration.txt, access.json, recovery.json,
outage.json and unit-tests.txt. Recovery timing is a local observation, not a
service-level guarantee. The retained dump is in ignored artifacts/fresh-recovery.

Limits: Airflow `dag.test()` uses the real task runtime and PostgreSQL, but does
not validate scheduled-executor throughput. No public endpoint, cloud VM,
managed secret store, off-host disaster recovery, production hardening or GitHub
Actions execution has been verified. All payment data is synthetic.
