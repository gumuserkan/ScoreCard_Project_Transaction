import sys
import types

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

from wallet_features.pricing import PriceService


def test_price_service_enabled_when_api_key_present(monkeypatch):
    monkeypatch.delenv("ENABLE_PRICE_SERVICE", raising=False)
    service = PriceService(api_key="dummy-key")
    assert service.enabled is True


@pytest.mark.parametrize("flag", ["0", "false", "no", "off"])
def test_price_service_respects_disable_flag(monkeypatch, flag):
    monkeypatch.setenv("ENABLE_PRICE_SERVICE", flag)
    service = PriceService(api_key="dummy-key")
    assert service.enabled is False
