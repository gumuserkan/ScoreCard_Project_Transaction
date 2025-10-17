import asyncio
import sys
import types

from datetime import datetime, timedelta, timezone

import pytest

# Provide a lightweight aiohttp stub for test environments without the package.
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

from wallet_features.pricing import COINGECKO_PROVIDER, COINMARKETCAP_PROVIDER, PriceService, TokenPrice


def test_price_service_enabled_when_api_key_present(monkeypatch):
    monkeypatch.delenv("ENABLE_PRICE_SERVICE", raising=False)
    service = PriceService(api_key="dummy-key")
    assert service.enabled is True


@pytest.mark.parametrize("flag", ["0", "false", "no", "off"])
def test_price_service_respects_disable_flag(monkeypatch, flag):
    monkeypatch.setenv("ENABLE_PRICE_SERVICE", flag)
    service = PriceService(api_key="dummy-key")
    assert service.enabled is False


def test_price_service_alternates_providers(monkeypatch):
    service = PriceService(api_key="dummy-key")
    calls = []

    async def fake_fetch(provider, contract_address, network, timestamp):
        calls.append(provider)
        return TokenPrice(usd=1.0, timestamp=timestamp)

    monkeypatch.setattr(service, "_fetch_with_provider", fake_fetch)

    async def runner():
        now = datetime.now(timezone.utc)
        await service.get_price(
            contract_address=None,
            network="eth-mainnet",
            timestamp=now,
            wallet="wallet-1",
        )
        await service.get_price(
            contract_address=None,
            network="eth-mainnet",
            timestamp=now + timedelta(hours=1),
            wallet="wallet-2",
        )

    asyncio.run(runner())
    assert calls[:2] == [COINMARKETCAP_PROVIDER, COINGECKO_PROVIDER]
