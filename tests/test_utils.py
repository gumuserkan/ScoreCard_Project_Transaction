import importlib
import sys
import types
from pathlib import Path

import pytest

from wallet_features.utils import load_wallets


class DummySheet:
    def __init__(self, rows):
        self._rows = rows

    def iter_rows(self, values_only=True):
        for row in self._rows:
            yield row


class DummyWorkbook:
    def __init__(self, rows):
        self.active = DummySheet(rows)

    def close(self):
        pass


def install_openpyxl_stub(monkeypatch, rows):
    module = types.ModuleType("openpyxl")

    def load_workbook(filename, read_only=True, data_only=True):
        return DummyWorkbook(rows)

    module.load_workbook = load_workbook
    monkeypatch.setitem(sys.modules, "openpyxl", module)


@pytest.mark.parametrize(
    "values",
    [
        ["0x1234567890abcdef1234567890abcdef12345678", "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"],
        ["0x1234567890abcdef1234567890abcdef12345678", "example.eth"],
    ],
)
def test_load_wallets_excel_without_header(monkeypatch, tmp_path: Path, values):
    rows = [(value,) for value in values]
    install_openpyxl_stub(monkeypatch, rows)
    path = tmp_path / "wallets.xlsx"
    path.write_bytes(b"dummy")

    loaded = load_wallets(path, None)
    expected = sorted(value.lower() for value in values)
    assert loaded == expected


def test_load_wallets_excel_skips_initial_empty_rows(monkeypatch, tmp_path: Path):
    rows = [(None,), ("0x1234567890abcdef1234567890abcdef12345678",), ("example.eth",)]
    install_openpyxl_stub(monkeypatch, rows)
    path = tmp_path / "wallets.xlsx"
    path.write_bytes(b"dummy")

    loaded = load_wallets(path, None)
    expected = ["0x1234567890abcdef1234567890abcdef12345678", "example.eth"]
    assert loaded == expected


def test_cli_invocation_uses_typer_app():
    pytest.importorskip("typer")
    module = importlib.import_module("wallet_features.cli")
    assert hasattr(module, "app"), "cli module should expose a Typer app"
    assert callable(module.app), "Typer app should be callable"
