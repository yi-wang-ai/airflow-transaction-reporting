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

from __future__ import annotations

import importlib.util
from decimal import Decimal
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "transaction_reporting", Path(__file__).parents[1] / "dags" / "transaction_reporting.py"
)
pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pipeline)
DAY = "2026-10-01"


@pytest.fixture
def transaction():
    return {
        "tx_id": "A8098C1A-F86E-11DA-BD1A-00112444BE1E",
        "account_id": 1001,
        "merchant": "Coffee Shop",
        "amount_cents": 1999,
        "currency": "AUD",
        "status": "COMPLETED",
        "created_at": "2026-10-02T01:00:00+13:00",
    }


def test_replay_is_deterministic_and_another_date_has_distinct_ids():
    first = pipeline.make_transactions(DAY)
    replay = pipeline.make_transactions(DAY)
    another_day = pipeline.make_transactions("2026-10-02")
    assert first == replay
    assert len(first) == 200
    assert len({tx["tx_id"] for tx in first}) == 200
    assert {tx["tx_id"] for tx in first}.isdisjoint(tx["tx_id"] for tx in another_day)
    assert {tx["currency"] for tx in first} == {"AUD", "EUR", "USD"}
    assert {tx["status"] for tx in first} == {"COMPLETED", "FAILED"}
    assert pipeline.validate_batch(first, DAY) == first


def test_validation_normalizes_uuid_and_timezone_without_mutating_input(transaction):
    result = pipeline.validate_batch([transaction], DAY)[0]
    assert result["tx_id"] == "a8098c1a-f86e-11da-bd1a-00112444be1e"
    assert result["created_at"] == "2026-10-01T12:00:00+00:00"
    assert transaction["created_at"] == "2026-10-02T01:00:00+13:00"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("amount_cents", -100, "amount_cents"),
        ("amount_cents", 0, "amount_cents"),
        ("amount_cents", 19.99, "amount_cents"),
        ("amount_cents", True, "amount_cents"),
        ("amount_cents", 2_147_483_648, "amount_cents"),
        ("account_id", 0, "account_id"),
        ("merchant", " ", "merchant"),
        ("currency", "GBP", "currency or status"),
        ("status", "PENDING", "currency or status"),
        ("tx_id", "invalid", "invalid UUID"),
        ("created_at", "invalid", "invalid timestamp"),
        ("created_at", "2026-10-01T12:00:00", "timezone"),
        ("created_at", "2026-10-01T00:00:00+13:00", "outside UTC reporting date"),
    ],
)
def test_invalid_transaction_is_rejected(transaction, field, value, message):
    with pytest.raises(ValueError, match=message):
        pipeline.validate_batch([{**transaction, field: value}], DAY)


def test_duplicate_ids_are_rejected_after_normalization(transaction):
    with pytest.raises(ValueError, match="duplicate transaction ID"):
        pipeline.validate_batch([transaction, {**transaction, "tx_id": transaction["tx_id"].lower()}], DAY)


def test_empty_batch_is_rejected():
    with pytest.raises(ValueError, match="No transactions"):
        pipeline.validate_batch([], DAY)


def test_missing_field_is_rejected(transaction):
    del transaction["amount_cents"]
    with pytest.raises(ValueError, match="missing fields"):
        pipeline.validate_batch([transaction], DAY)


def test_currency_aggregation_excludes_failed_payments_and_rounds_decimal_cents(transaction):
    batch = [
        {**transaction, "amount_cents": 1},
        {**transaction, "amount_cents": 2},
        {**transaction, "amount_cents": 2},
        {**transaction, "amount_cents": 9999, "status": "FAILED"},
        {**transaction, "amount_cents": 500, "currency": "USD"},
    ]
    assert pipeline.summarize_batch(batch) == [
        ("AUD", 3, 5, Decimal("1.67")),
        ("USD", 1, 500, Decimal("500.00")),
    ]


def test_all_failed_payments_produce_no_completed_currency_groups(transaction):
    assert pipeline.summarize_batch([{**transaction, "status": "FAILED"}]) == []


@pytest.mark.parametrize("day", ["2026-02-30", "20261001"])
def test_invalid_or_noncanonical_report_date_is_rejected(day):
    with pytest.raises(ValueError, match="day is out of range|YYYY-MM-DD"):
        pipeline.make_transactions(day)
