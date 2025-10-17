"""Token pricing services."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from wallet_features.utils import async_retry, getenv

LOGGER = logging.getLogger(__name__)

COINMARKETCAP_BASE = "https://pro-api.coinmarketcap.com"
COINMARKETCAP_API_ENV = "COINMARKETCAP_API_KEY"
USD = "USD"
ETH_CMC_ID = 1027


@dataclass
class TokenPrice:
    """Represents a token price at a timestamp."""

    usd: float
    timestamp: datetime


class PriceService:
    """Service providing token and native prices."""

    def __init__(
        self,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: int = 30,
        api_key: Optional[str] = None,
    ):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = session
        self._session_owner = session is None
        self._price_cache: Dict[str, TokenPrice] = {}
        self.api_key = api_key or getenv(COINMARKETCAP_API_ENV)
        self._missing_key_logged = False
        self._contract_id_cache: Dict[str, Optional[int]] = {}

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
        if not self.api_key:
            if not self._missing_key_logged:
                LOGGER.warning(
                    "CoinMarketCap API key not configured; price requests will be skipped"
                )
                self._missing_key_logged = True
            return {}
        session = await self._ensure_session()
        headers = {
            "X-CMC_PRO_API_KEY": self.api_key,
            "Accepts": "application/json",
        }
        async with session.get(url, params=params, headers=headers) as resp:
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

    async def _fetch_contract_price(
        self, contract_address: str, timestamp: datetime
    ) -> Optional[TokenPrice]:
        token_id = await self._get_contract_token_id(contract_address)
        if token_id is None:
            return None
        try:
            return await self._fetch_price_for_id(token_id, timestamp)
        except Exception as exc:  # pragma: no cover - runtime only
            LOGGER.warning("Failed to fetch contract price %s: %s", contract_address, exc)
            return None

    async def _fetch_eth_price(self, timestamp: datetime) -> Optional[TokenPrice]:
        try:
            return await self._fetch_price_for_id(ETH_CMC_ID, timestamp)
        except Exception as exc:  # pragma: no cover - runtime only
            LOGGER.warning("Failed to fetch ETH price: %s", exc)
            return None

    async def _fetch_price_for_id(self, token_id: int, timestamp: datetime) -> Optional[TokenPrice]:
        price = await self._fetch_historical_price({"id": str(token_id)}, timestamp)
        if price:
            return price
        return await self._fetch_latest_price({"id": str(token_id)}, timestamp)

    async def _fetch_historical_price(
        self, base_params: Dict[str, str], timestamp: datetime
    ) -> Optional[TokenPrice]:
        dt = timestamp.astimezone(timezone.utc)
        params = {
            **base_params,
            "time_start": (dt - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S"),
            "time_end": (dt + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S"),
            "interval": "hourly",
            "convert": USD,
            "count": "1",
        }
        url = f"{COINMARKETCAP_BASE}/v2/cryptocurrency/quotes/historical"
        data = await self._request(url, params=params)
        if not data:
            return None
        return self._parse_historical_price(data, timestamp)

    async def _fetch_latest_price(
        self, base_params: Dict[str, str], fallback_timestamp: datetime
    ) -> Optional[TokenPrice]:
        params = {**base_params, "convert": USD}
        url = f"{COINMARKETCAP_BASE}/v2/cryptocurrency/quotes/latest"
        data = await self._request(url, params=params)
        if not data:
            return None
        return self._parse_latest_price(data, fallback_timestamp)

    async def _get_contract_token_id(self, contract_address: str) -> Optional[int]:
        normalized = contract_address.lower()
        if normalized in self._contract_id_cache:
            return self._contract_id_cache[normalized]
        url = f"{COINMARKETCAP_BASE}/v2/cryptocurrency/info"
        params = {"address": normalized}
        try:
            data = await self._request(url, params=params)
        except Exception as exc:  # pragma: no cover - runtime only
            LOGGER.warning("Failed to fetch metadata for %s: %s", contract_address, exc)
            self._contract_id_cache[normalized] = None
            return None
        token_id = self._parse_contract_token_id(data, normalized)
        self._contract_id_cache[normalized] = token_id
        return token_id

    def _parse_contract_token_id(
        self, data: Dict[str, Any], normalized_address: str
    ) -> Optional[int]:
        payload = data.get("data") if isinstance(data, dict) else None
        if not isinstance(payload, dict):
            return None
        for value in payload.values():
            entries: List[Dict[str, Any]]
            if isinstance(value, list):
                entries = [item for item in value if isinstance(item, dict)]
            elif isinstance(value, dict):
                entries = [value]
            else:
                continue
            for entry in entries:
                platform = entry.get("platform")
                token_address = ""
                if isinstance(platform, dict):
                    token_address = str(platform.get("token_address", "")).lower()
                if token_address != normalized_address:
                    continue
                token_id = entry.get("id")
                try:
                    return int(token_id)
                except (TypeError, ValueError):
                    continue
        return None

    def _parse_historical_price(
        self, data: Dict[str, Any], fallback_timestamp: datetime
    ) -> Optional[TokenPrice]:
        quotes = self._extract_quote_entries(data)
        if not quotes:
            return None
        best: Optional[Tuple[datetime, float]] = None
        best_diff: Optional[float] = None
        for quote in quotes:
            parsed = self._extract_usd_price(quote)
            if not parsed:
                continue
            quote_ts, price = parsed
            ts = quote_ts or fallback_timestamp
            diff = abs((ts - fallback_timestamp).total_seconds())
            if best is None or diff < (best_diff or float("inf")):
                best = (ts, price)
                best_diff = diff
        if not best:
            return None
        ts, price = best
        return TokenPrice(usd=price, timestamp=ts)

    def _parse_latest_price(
        self, data: Dict[str, Any], fallback_timestamp: datetime
    ) -> Optional[TokenPrice]:
        entries = self._extract_latest_entries(data)
        for entry in entries:
            quote = entry.get("quote") if isinstance(entry, dict) else None
            if not isinstance(quote, dict):
                continue
            usd_info = quote.get(USD)
            if not isinstance(usd_info, dict):
                continue
            price = usd_info.get("price")
            if price is None:
                continue
            try:
                price_value = float(price)
            except (TypeError, ValueError):
                continue
            ts = self._parse_cmc_timestamp(usd_info.get("last_updated")) or fallback_timestamp
            return TokenPrice(usd=price_value, timestamp=ts)
        return None

    def _extract_quote_entries(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        payload = data.get("data") if isinstance(data, dict) else None
        quotes: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            if isinstance(payload.get("quotes"), list):
                quotes.extend(
                    [quote for quote in payload.get("quotes", []) if isinstance(quote, dict)]
                )
            else:
                for value in payload.values():
                    if isinstance(value, dict) and isinstance(value.get("quotes"), list):
                        quotes.extend(
                            [quote for quote in value.get("quotes", []) if isinstance(quote, dict)]
                        )
        elif isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and isinstance(item.get("quotes"), list):
                    quotes.extend(
                        [quote for quote in item.get("quotes", []) if isinstance(quote, dict)]
                    )
        return quotes

    def _extract_latest_entries(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        payload = data.get("data") if isinstance(data, dict) else None
        entries: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            if "quote" in payload:
                entries.append(payload)
            else:
                for value in payload.values():
                    if isinstance(value, dict) and "quote" in value:
                        entries.append(value)
        elif isinstance(payload, list):
            entries.extend([item for item in payload if isinstance(item, dict)])
        return entries

    def _extract_usd_price(
        self, quote: Dict[str, Any]
    ) -> Optional[Tuple[Optional[datetime], float]]:
        if not isinstance(quote, dict):
            return None
        quote_data = quote.get("quote")
        if not isinstance(quote_data, dict):
            return None
        usd_info = quote_data.get(USD)
        if not isinstance(usd_info, dict):
            return None
        price = usd_info.get("price")
        if price is None:
            return None
        try:
            price_value = float(price)
        except (TypeError, ValueError):
            return None
        timestamp_value = (
            usd_info.get("timestamp")
            or usd_info.get("last_updated")
            or quote.get("timestamp")
            or quote.get("time_close")
            or quote.get("time_open")
        )
        ts = self._parse_cmc_timestamp(timestamp_value)
        return ts, price_value

    @staticmethod
    def _parse_cmc_timestamp(value: Optional[str]) -> Optional[datetime]:
        if not value or not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
