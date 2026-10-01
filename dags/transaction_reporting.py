# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
"""A small synthetic-payment reporting exercise; all amounts are integer cents."""

from __future__ import annotations

import logging
import random
import uuid
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

import pendulum

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, get_current_context, task

CONN_ID = "payments_db"
MERCHANTS = ("Coffee Shop", "Supermarket", "Bookstore", "Electronics", "Restaurant")
CURRENCIES = ("AUD", "USD", "EUR")
STATUSES = ("COMPLETED", "FAILED")
LOG = logging.getLogger(__name__)
FIELDS = ("tx_id", "account_id", "merchant", "amount_cents", "currency", "status", "created_at")

CREATE_TABLES = """
CREATE SCHEMA IF NOT EXISTS payment_demo;
CREATE TABLE IF NOT EXISTS payment_demo.raw_transactions (
    tx_id UUID PRIMARY KEY,
    account_id INTEGER NOT NULL CHECK (account_id > 0),
    merchant TEXT NOT NULL CHECK (length(trim(merchant)) > 0),
    amount_cents INTEGER NOT NULL CHECK (amount_cents > 0),
    currency CHAR(3) NOT NULL CHECK (currency IN ('AUD', 'USD', 'EUR')),
    status TEXT NOT NULL CHECK (status IN ('COMPLETED', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS payment_demo.daily_transaction_report (
    report_date DATE NOT NULL,
    currency CHAR(3) NOT NULL,
    transaction_count INTEGER NOT NULL CHECK (transaction_count > 0),
    total_amount_cents BIGINT NOT NULL CHECK (total_amount_cents > 0),
    average_amount_cents NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (report_date, currency)
);
"""

BUILD_REPORT = [
    "DELETE FROM payment_demo.daily_transaction_report WHERE report_date = %(report_date)s::date",
    """
    INSERT INTO payment_demo.daily_transaction_report
        (report_date, currency, transaction_count, total_amount_cents, average_amount_cents)
    SELECT %(report_date)s::date, currency, COUNT(*), SUM(amount_cents), ROUND(AVG(amount_cents), 2)
    FROM payment_demo.raw_transactions
    WHERE created_at >= (%(report_date)s::date::timestamp AT TIME ZONE 'UTC')
      AND created_at < ((%(report_date)s::date + 1)::timestamp AT TIME ZONE 'UTC')
      AND status = 'COMPLETED'
    GROUP BY currency
    """,
]


def parse_report_date(value: str) -> date:
    result = date.fromisoformat(value)
    if result.isoformat() != value:
        raise ValueError("report_date must be YYYY-MM-DD")
    return result


def make_transactions(report_date: str) -> list[dict]:
    day = parse_report_date(report_date)
    rng = random.Random(f"payment-demo-v1:{day}")
    created_at = f"{day}T12:00:00+00:00"
    return [
        {
            "tx_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment-demo-v1:{day}:{index}")),
            "account_id": rng.randint(1000, 1100),
            "merchant": rng.choice(MERCHANTS),
            "amount_cents": rng.randint(100, 100_000),
            "currency": rng.choice(CURRENCIES),
            "status": rng.choice(("COMPLETED", "COMPLETED", "COMPLETED", "FAILED")),
            "created_at": created_at,
        }
        for index in range(200)
    ]


def validate_batch(transactions: list[dict], report_date: str) -> list[dict]:
    day = parse_report_date(report_date)
    if not isinstance(transactions, list) or not transactions:
        raise ValueError("No transactions supplied")
    seen_ids = set()
    validated = []
    for index, tx in enumerate(transactions):
        if not isinstance(tx, dict) or set(tx) != set(FIELDS):
            raise ValueError(f"Transaction {index}: unexpected or missing fields")
        try:
            tx_id = str(uuid.UUID(tx["tx_id"]))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError(f"Transaction {index}: invalid UUID") from exc
        if tx_id in seen_ids:
            raise ValueError(f"Transaction {index}: duplicate transaction ID")
        for field in ("account_id", "amount_cents"):
            if type(tx[field]) is not int or not 0 < tx[field] <= 2_147_483_647:
                raise ValueError(f"Transaction {index}: {field} must be a positive PostgreSQL integer")
        if not isinstance(tx["merchant"], str) or not tx["merchant"].strip():
            raise ValueError(f"Transaction {index}: merchant must be non-empty text")
        if tx["currency"] not in CURRENCIES or tx["status"] not in STATUSES:
            raise ValueError(f"Transaction {index}: unsupported currency or status")
        try:
            created_at = datetime.fromisoformat(tx["created_at"])
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Transaction {index}: invalid timestamp") from exc
        if created_at.tzinfo is None or created_at.utcoffset() is None:
            raise ValueError(f"Transaction {index}: timestamp must have a timezone")
        created_at = created_at.astimezone(timezone.utc)
        if created_at.date() != day:
            raise ValueError(f"Transaction {index}: timestamp outside UTC reporting date")
        validated.append({**tx, "tx_id": tx_id, "created_at": created_at.isoformat()})
        seen_ids.add(tx_id)
    return validated


def summarize_batch(transactions: list[dict]) -> list[tuple]:
    grouped = {}
    for tx in transactions:
        if tx["status"] == "COMPLETED":
            count, total = grouped.get(tx["currency"], (0, 0))
            grouped[tx["currency"]] = (count + 1, total + tx["amount_cents"])
    return [
        (currency, count, total, (Decimal(total) / count).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        for currency, (count, total) in sorted(grouped.items())
    ]


@dag(
    dag_id="transaction_reporting",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["portfolio", "payments", "postgres"],
    params={
        "report_date": Param("2026-10-01", type="string", format="date"),
        "transactions": Param(
            None,
            type=["null", "array"],
            description="Optional input batch for validation exercises; null generates 200 synthetic payments.",
        ),
    },
)
def transaction_reporting():
    create_tables = SQLExecuteQueryOperator(
        task_id="create_tables", conn_id=CONN_ID, sql=CREATE_TABLES, split_statements=True
    )

    @task
    def generate_transactions() -> list[dict]:
        params = get_current_context()["params"]
        batch = params["transactions"]
        if batch is None:
            batch = make_transactions(params["report_date"])
        LOG.info("Prepared %d transactions for UTC date %s", len(batch), params["report_date"])
        return batch

    @task
    def validate_transactions(transactions: list[dict]) -> list[dict]:
        batch = validate_batch(transactions, get_current_context()["params"]["report_date"])
        LOG.info("Validated %d transactions", len(batch))
        return batch

    @task
    def load_transactions(transactions: list[dict]) -> int:
        conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO payment_demo.raw_transactions
                        (tx_id, account_id, merchant, amount_cents, currency, status, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tx_id) DO NOTHING
                    """,
                    [tuple(tx[field] for field in FIELDS) for tx in transactions],
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        LOG.info("Processed %d rows using idempotent inserts", len(transactions))
        return len(transactions)

    build_report = SQLExecuteQueryOperator(
        task_id="build_daily_report",
        conn_id=CONN_ID,
        sql=BUILD_REPORT,
        parameters={"report_date": "{{ params.report_date }}"},
        autocommit=False,
    )

    @task
    def quality_check(transactions: list[dict]) -> dict:
        day = get_current_context()["params"]["report_date"]
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        records = hook.get_records(
            """
            SELECT tx_id::text, account_id, merchant, amount_cents, currency, status, created_at
            FROM payment_demo.raw_transactions
            WHERE created_at >= (%s::date::timestamp AT TIME ZONE 'UTC')
              AND created_at < ((%s::date + 1)::timestamp AT TIME ZONE 'UTC')
            ORDER BY tx_id::text
            """,
            parameters=(day, day),
        )
        stored = [(*row[:-1], row[-1].astimezone(timezone.utc).isoformat()) for row in records]
        expected = sorted(tuple(tx[field] for field in FIELDS) for tx in transactions)
        if stored != expected:
            raise ValueError("Persisted transactions do not exactly match the validated batch")
        report = hook.get_records(
            """
            SELECT currency, transaction_count, total_amount_cents, average_amount_cents
            FROM payment_demo.daily_transaction_report
            WHERE report_date = %s::date ORDER BY currency
            """,
            parameters=(day,),
        )
        expected_report = summarize_batch(transactions)
        if report != expected_report:
            raise ValueError(f"Report mismatch: expected {expected_report!r}, received {report!r}")
        result = {"report_date": day, "raw_rows": len(records), "currency_groups": len(report)}
        LOG.info("Quality check passed: %s; exact per-currency reconciliation: %s", result, report)
        return result

    generated = generate_transactions()
    validated = validate_transactions(generated)
    loaded = load_transactions(validated)
    create_tables >> generated
    loaded >> build_report >> quality_check(validated)


transaction_reporting_dag = transaction_reporting()
