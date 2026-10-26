"""Tests for ecom_lakehouse.bronze."""

import pytest

from ecom_lakehouse import bronze


def test_load_all_builds_paths_and_table_names(monkeypatch):
    calls = []

    def fake_load(spark, source_path, table):
        calls.append((source_path, table))
        return 100

    monkeypatch.setattr(bronze, "load_to_bronze", fake_load)

    counts = bronze.load_all(None, "/vol/olist", "ecom_test", {"orders": "orders.csv"})

    assert calls == [("/vol/olist/orders.csv", "ecom_test.bronze.orders")]
    assert counts == {"orders": 100}


def test_load_all_continues_after_failure_then_raises(monkeypatch):
    loaded = []

    def fake_load(spark, source_path, table):
        if table.endswith("bad_table"):
            raise FileNotFoundError(source_path)
        loaded.append(table)
        return 100

    monkeypatch.setattr(bronze, "load_to_bronze", fake_load)
    tables = {"orders": "orders.csv", "bad_table": "missing.csv", "sellers": "sellers.csv"}

    with pytest.raises(RuntimeError, match="bad_table"):
        bronze.load_all(None, "/vol/olist", "ecom_test", tables)

    assert loaded == ["ecom_test.bronze.orders", "ecom_test.bronze.sellers"]
