import asyncio
import sys
import types
from datetime import datetime, timedelta, timezone

import pytest

# Provide a lightweight aiohttp stub for tests when the real library is unavailable.
if "aiohttp" not in sys.modules:  # pragma: no cover - test helper
    aiohttp_stub = types.ModuleType("aiohttp")

    class ClientTimeout:  # pragma: no cover - simple stub
        def __init__(self, *args, **kwargs):
            pass

    class ClientSession:  # pragma: no cover - simple stub
        def __init__(self, *args, **kwargs):
            pass

        async def close(self):
            pass

    aiohttp_stub.ClientTimeout = ClientTimeout
    aiohttp_stub.ClientSession = ClientSession
    sys.modules["aiohttp"] = aiohttp_stub

from wallet_features.features import FeatureCalculator, TransferEvent
from wallet_features.pricing import TokenPrice


class DummyClient:
    async def get_token_metadata(self, contract_address: str):
        return {"decimals": 18}


class DummyPriceService:
    async def get_price(self, *, contract_address, network, timestamp):
        return TokenPrice(usd=1000.0, timestamp=timestamp)


def test_decode_value_uses_decimals(monkeypatch):
    client = DummyClient()
    prices = DummyPriceService()
    calculator = FeatureCalculator(client, prices)
    event = TransferEvent(
        tx_hash="0x1",
        unique_id="0x1-1",
        timestamp=datetime.now(timezone.utc),
        category="erc20",
        asset="TEST",
        raw_value="0xde0b6b3a7640000",  # 1 token with 18 decimals
        value=None,
        raw_contract={"address": "0xabc", "decimals": "18"},
        from_address="0xwallet",
        to_address="0xother",
        direction="out",
        raw={},
    )
    amount = asyncio.run(calculator._decode_value(event))
    assert amount == pytest.approx(1.0)


def test_compute_wallet_features(monkeypatch):
    now = datetime.now(timezone.utc)
    client = DummyClient()
    prices = DummyPriceService()
    calculator = FeatureCalculator(client, prices)

    event_recent = TransferEvent(
        tx_hash="0xabc",
        unique_id="0xabc-1",
        timestamp=now,
        category="external",
        asset="ETH",
        raw_value=None,
        value=1.0,
        raw_contract={},
        from_address="0xwallet",
        to_address="0xother",
        direction="out",
        raw={},
    )
    event_old = TransferEvent(
        tx_hash="0xdef",
        unique_id="0xdef-1",
        timestamp=now - timedelta(days=40),
        category="external",
        asset="ETH",
        raw_value=None,
        value=1.0,
        raw_contract={},
        from_address="0xother",
        to_address="0xwallet",
        direction="in",
        raw={},
    )

    async def fake_fetch(wallet):
        return [event_recent, event_old], [event_recent, event_old]

    async def fake_receipt(tx_hash):
        return {"gasUsed": hex(21000), "effectiveGasPrice": hex(100_000_000_000)}

    monkeypatch.setattr(calculator, "fetch_wallet_data", fake_fetch)
    monkeypatch.setattr(calculator, "get_transaction_receipt", fake_receipt)

    features = asyncio.run(calculator.compute_wallet_features("0xwallet"))
    assert features["Total Tx Count (1M)"] == 1
    assert features["Total Tx Count (3M)"] == 2
    assert features["Total Tx Volume (1M)"] == pytest.approx(1000.0)
    assert features["Total Tx Volume (3M)"] == pytest.approx(2000.0)
    assert features["Monthly Tx Count Avg (12M)"] == pytest.approx(2 / 12, rel=1e-3)
    assert features["Total Gas Fee (USD)"] == pytest.approx(2.1, rel=1e-3)
    assert "TRANSFER" in features["Tx Types (Last 250 Tx)"]
