"""Token pricing services."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp

from wallet_features.utils import async_retry

LOGGER = logging.getLogger(__name__)

COINGECKO_BASE = "https://api.coingecko.com/api/v3"


@dataclass
class TokenPrice:
    """Represents a token price at a timestamp."""

    usd: float
    timestamp: datetime


class PriceService:
    """Service providing token and native prices."""

    def __init__(self, session: Optional[aiohttp.ClientSession] = None, timeout: int = 30):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = session
        self._session_owner = session is None
        self._price_cache: Dict[str, TokenPrice] = {}

    async def __aenter__(self) -> "PriceService":
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session and self._session_owner:
            await self._session.close()

    async def close(self) -> None:
        if self._session and self._session_owner:
            await self._session.close()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    def _cache_key(self, asset_id: str, date_key: str) -> str:
        return f"{asset_id}:{date_key}"

    @staticmethod
    def _date_key(timestamp: datetime) -> str:
        dt = timestamp.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d-%H")

    async def get_price(
        self,
        *,
        contract_address: Optional[str],
        network: str,
        timestamp: datetime,
    ) -> Optional[TokenPrice]:
        date_key = self._date_key(timestamp)
        asset_id = contract_address.lower() if contract_address else "eth"
        cache_key = self._cache_key(asset_id, date_key)
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]
        if contract_address:
            price = await self._fetch_contract_price(contract_address, timestamp)
            if price is None:
                LOGGER.debug("Falling back to native price for %s", contract_address)
                price = await self._fetch_eth_price(timestamp)
        else:
            price = await self._fetch_eth_price(timestamp)
        if price:
            self._price_cache[cache_key] = price
        return price

    @async_retry()
    async def _request(self, url: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        session = await self._ensure_session()
        async with session.get(url, params=params) as resp:
            if resp.status >= 500:
                raise RuntimeError(f"Price service error {resp.status}")
            if resp.status == 429:
                raise RuntimeError("Price rate limited")
            if resp.status == 404:
                text = await resp.text()
                LOGGER.debug(
                    "Price request not found for %s params=%s: %s",
                    url,
                    params,
                    text.strip(),
                )
                return {}
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"Price error {resp.status}: {text}")
            return await resp.json()

    async def _fetch_contract_price(self, contract_address: str, timestamp: datetime) -> Optional[TokenPrice]:
        date = timestamp.astimezone(timezone.utc).strftime("%d-%m-%Y")
        url = f"{COINGECKO_BASE}/coins/ethereum/contract/{contract_address}/history"
        try:
            data = await self._request(url, params={"date": date})
        except Exception as exc:  # pragma: no cover - runtime only
            LOGGER.warning("Failed to fetch contract price %s: %s", contract_address, exc)
            return None
        market_data = data.get("market_data") if isinstance(data, dict) else None
        if not market_data:
            return None
        current_price = market_data.get("current_price", {})
        usd = current_price.get("usd")
        if usd is None:
            return None
        return TokenPrice(usd=float(usd), timestamp=timestamp)

    async def _fetch_eth_price(self, timestamp: datetime) -> Optional[TokenPrice]:
        date = timestamp.astimezone(timezone.utc).strftime("%d-%m-%Y")
        url = f"{COINGECKO_BASE}/coins/ethereum/history"
        try:
            data = await self._request(url, params={"date": date})
        except Exception as exc:  # pragma: no cover - runtime only
            LOGGER.warning("Failed to fetch ETH price: %s", exc)
            return None
        market_data = data.get("market_data") if isinstance(data, dict) else None
        if not market_data:
            return None
        current_price = market_data.get("current_price", {})
        usd = current_price.get("usd")
        if usd is None:
            return None
        return TokenPrice(usd=float(usd), timestamp=timestamp)
