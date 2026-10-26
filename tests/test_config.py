from pathlib import Path

import pytest

from ecom_lakehouse import config

REPO_ROOT = Path(__file__).parent.parent


def test_load_tables_returns_mapping(tmp_path):
    yml = tmp_path / "sources.yml"
    yml.write_text("tables:\n  orders: orders.csv\n  sellers: sellers.csv\n")

    result = config.load_tables(str(yml))

    assert result == {"orders": "orders.csv", "sellers": "sellers.csv"}


def test_load_tables_without_tables_key_raises(tmp_path):
    yml = tmp_path / "sources.yml"
    yml.write_text("something_else:\n  a: b\n")

    with pytest.raises(KeyError):
        config.load_tables(str(yml))


def test_real_sources_file_has_all_nine_tables():
    tables = config.load_tables(str(REPO_ROOT / "conf" / "sources.yml"))

    assert len(tables) == 9
    assert all(name.endswith(".csv") for name in tables.values())