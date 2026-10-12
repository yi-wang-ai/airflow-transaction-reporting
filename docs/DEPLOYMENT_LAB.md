# Deployment, monitoring and recovery teaching lab

A single-host educational Airflow deployment with synthetic transactions.
It demonstrates container deployment and operational checks. It is not a public
production service, high-availability cluster or completed cloud deployment.

## Start on Docker Desktop or an existing Linux VM

From the repository root, with Docker Compose v2 and at least 4 GB Docker memory:

```bash
python3 scripts/init_secrets.py
docker compose -f deployment/compose.yaml up -d --wait --wait-timeout 240
```

The Airflow and PostgreSQL images are pinned by release and digest. The first
startup initialises two separate databases and non-superuser roles. A short
root init container sets ownership of the Airflow state volume; Airflow itself
runs as the image's non-root user. PostgreSQL is not published to the host.
Airflow binds only to `127.0.0.1:18080`.

Open http://127.0.0.1:18080. The teacher account uses Airflow SimpleAuthManager's
generated password. Read it locally from the state volume when logging in:

```bash
docker compose -f deployment/compose.yaml exec airflow \
  cat /opt/airflow/state/simple_auth_manager_passwords.json.generated
```

Do not paste that output into public evidence. File-backed database, Fernet and
JWT secrets live in ignored `.secrets/`. The directory is owner-only; the mounted
individual files are readable by container users. Compose file secrets are local
files, not a managed cloud secret store. Protect the host and Docker socket.
Re-running the generator preserves existing keys. Replacing password files alone
does not rotate credentials already stored in a PostgreSQL volume.

## Verification

```bash
docker compose -f deployment/compose.yaml exec -T airflow python /opt/lab/check_access.py
docker compose -f deployment/compose.yaml exec -T airflow python /opt/lab/integration_check.py
docker compose -f deployment/compose.yaml run --rm --no-deps --entrypoint bash airflow \
  -c 'python -m pip install pytest==8.4.2 && python -m pytest /opt/airflow/tests -q -p no:cacheprovider'
python3 scripts/recovery_drill.py --output artifacts/recovery-01
```

The integration runner uses Airflow's real `dag.test()` execution and PostgreSQL,
not mocked hooks. It checks baseline success, unchanged replay and rejected-input
isolation. It expects the dedicated demo database to contain only this exercise's
200 transactions. A deliberately rejected batch creates a failed DAG run; that
is the expected result of this test. These checks do not establish scheduled
executor throughput or production load performance.

The recovery drill writes a PostgreSQL custom-format dump, creates a new random
restore database, compares complete-table fingerprints and counts, confirms the
source stayed unchanged, then removes only its temporary restore database.
The dump and SHA256 remain in the new output directory. It refuses empty data.
This is a logical restore on the same server, not recovery from total VM loss.
Do not modify data while taking this drill's before/after snapshots.

## Monitoring and a controlled outage

```bash
docker compose -f deployment/compose.yaml ps
docker compose -f deployment/compose.yaml exec -T airflow python /opt/lab/healthcheck.py
python3 scripts/outage_drill.py --output artifacts/outage-01
```

Health checks inspect the metadata database and scheduler heartbeat. The outage
drill deliberately stops this lab's PostgreSQL service and restarts it in a
`finally` block. Run only when no DAG is active. It verifies detection and recovery;
it is not a task-retry or data-loss experiment. Database unavailability affects
both metadata and payment data because this small lab shares one PostgreSQL server.

## Existing Linux cloud VM

Use the same Compose setup on a suitable existing VM. Keep port 18080 and 5432
closed externally. Access the loopback-only interface through an SSH tunnel:

```bash
ssh -N -L 18080:127.0.0.1:18080 YOUR_EXISTING_SSH_ALIAS
```

Record the VM OS, CPU/memory, image digests and actual checks before claiming cloud
deployment. No VM has been provisioned or paid for by these changes. This lab uses
`airflow standalone` and SimpleAuthManager; do not expose it as a production service.

## GitHub and teaching use

The supplied workflow repeats startup, privilege checks, real DAG runs, unit tests
and restore checks on Ubuntu, retaining non-secret artifacts. Its GitHub execution
has not been verified locally. Runtime credentials and Airflow startup logs are
excluded from uploaded artifacts because startup can print generated passwords.

Suggested ten-minute lesson: draw the service/database boundary; show normal and
replay runs; reject a negative amount; demonstrate a database outage and recovery;
finish with restore fingerprints. Ask students why container health, data quality
and recoverability are different checks. Materials are prepared; delivery is unverified.

Stop containers without deleting evidence or database volumes:

```bash
docker compose -f deployment/compose.yaml stop
```

Reference: [Airflow 3.1.0 Docker documentation](https://airflow.apache.org/docs/apache-airflow/3.1.0/howto/docker-compose/index.html).
The upstream guide also describes Compose as a learning setup, not a production guarantee.
