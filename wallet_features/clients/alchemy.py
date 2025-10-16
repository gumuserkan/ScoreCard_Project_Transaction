"""Alchemy API client."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import aiohttp

from wallet_features.utils import async_retry, parse_iso8601

LOGGER = logging.getLogger(__name__)


class AlchemyError(RuntimeError):
    """Raised for API errors."""


class AlchemyClient:
    """Minimal asynchronous client for the Alchemy API."""

    BASE_HOST = "g.alchemy.com"

    def __init__(
        self,
        api_key: str,
        network: str = "eth-mainnet",
        session: Optional[aiohttp.ClientSession] = None,
        timeout: int = 30,
    ) -> None:
        self.api_key = api_key
        self.network = network
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session = session
        self._session_owner = session is None
        self._semaphore = asyncio.Semaphore(10)

    @property
    def base_url(self) -> str:
        return f"https://{self.network}.{self.BASE_HOST}/v2/{self.api_key}"

    async def __aenter__(self) -> "AlchemyClient":
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

    @async_retry()
    async def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        session = await self._ensure_session()
        async with self._semaphore:
            async with session.post(self.base_url, json=payload) as resp:
                if resp.status >= 500:
                    raise AlchemyError(f"Alchemy server error {resp.status}")
                if resp.status == 429:
                    raise AlchemyError("Alchemy rate limited")
                if resp.status >= 400:
                    text = await resp.text()
                    raise AlchemyError(f"Alchemy error {resp.status}: {text}")
                data = await resp.json()
        if "error" in data:
            raise AlchemyError(data["error"])
        return data

    async def rpc(self, method: str, params: Iterable[Any]) -> Any:
        payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params)}
        data = await self._post(payload)
        return data.get("result")

    async def resolve_ens(self, name: str) -> Optional[str]:
        """Resolve ENS name to address."""

        try:
            result = await self.rpc("alchemy_getAddressFromENS", [name])
        except AlchemyError as exc:
            LOGGER.warning("Failed to resolve ENS %s: %s", name, exc)
            return None
        if isinstance(result, str) and result.startswith("0x"):
            return result.lower()
        return None

    async def get_token_metadata(self, contract_address: str) -> Optional[Dict[str, Any]]:
        try:
            result = await self.rpc("alchemy_getTokenMetadata", [contract_address])
        except AlchemyError as exc:
            LOGGER.warning("Failed metadata for %s: %s", contract_address, exc)
            return None
        if not isinstance(result, dict):
            return None
        return result

    async def get_asset_transfers(
        self,
        *,
        address: str,
        from_address: bool,
        max_count: int = 100,
        page_key: Optional[str] = None,
        category: Optional[List[str]] = None,
        order: str = "desc",
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "fromBlock": "0x0",
            "category": category
            or [
                "external",
                "internal",
                "erc20",
                "erc721",
                "erc1155",
                "specialnft",
            ],
            "withMetadata": True,
            "maxCount": hex(max_count),
            "order": order,
        }
        if from_address:
            params["fromAddress"] = address
        else:
            params["toAddress"] = address
        if page_key:
            params["pageKey"] = page_key
        result = await self.rpc("alchemy_getAssetTransfers", [params])
        if not isinstance(result, dict):
            raise AlchemyError("Unexpected transfers response")
        return result

    async def get_asset_transfers_paginated(
        self,
        address: str,
        *,
        from_address: bool,
        limit: Optional[int] = None,
        stop_timestamp: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch transfers for an address with pagination."""

        all_transfers: List[Dict[str, Any]] = []
        page_key: Optional[str] = None
        while True:
            result = await self.get_asset_transfers(
                address=address,
                from_address=from_address,
                page_key=page_key,
            )
            transfers = result.get("transfers", [])
            if not isinstance(transfers, list):
                break
            for transfer in transfers:
                metadata = transfer.get("metadata", {})
                ts = metadata.get("blockTimestamp")
                if ts:
                    transfer["_parsed_timestamp"] = parse_iso8601(ts)
                else:
                    transfer["_parsed_timestamp"] = None
            all_transfers.extend(transfers)
            if limit and len(all_transfers) >= limit:
                all_transfers = all_transfers[:limit]
                break
            if stop_timestamp and transfers:
                oldest = transfers[-1].get("_parsed_timestamp")
                if isinstance(oldest, datetime) and oldest < stop_timestamp:
                    break
            page_key = result.get("pageKey")
            if not page_key:
                break
        return all_transfers

    async def get_transaction_receipt(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        try:
            result = await self.rpc("eth_getTransactionReceipt", [tx_hash])
        except AlchemyError as exc:
            LOGGER.warning("Failed to get receipt %s: %s", tx_hash, exc)
            return None
        if not isinstance(result, dict):
            return None
        return result

    async def get_block(self, block_number: int) -> Optional[Dict[str, Any]]:
        try:
            result = await self.rpc("eth_getBlockByNumber", [hex(block_number), False])
        except AlchemyError as exc:
            LOGGER.warning("Failed to get block %s: %s", block_number, exc)
            return None
        if not isinstance(result, dict):
            return None
        return result

    async def get_block_by_timestamp(self, timestamp: int) -> Optional[Dict[str, Any]]:
        params = {"closest": "before", "timestamp": hex(timestamp)}
        try:
            result = await self.rpc("alchemy_getBlockByTimestamp", [params])
        except AlchemyError as exc:
            LOGGER.warning("Failed to get block for timestamp %s: %s", timestamp, exc)
            return None
        if not isinstance(result, dict):
            return None
        return result


async def gather_wallet_transfers(
    client: AlchemyClient,
    address: str,
    *,
    limit: Optional[int] = None,
    stop_timestamp: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Fetch incoming and outgoing transfers for an address."""

    outgoing, incoming = await asyncio.gather(
        client.get_asset_transfers_paginated(
            address=address,
            from_address=True,
            limit=limit,
            stop_timestamp=stop_timestamp,
        ),
        client.get_asset_transfers_paginated(
            address=address,
            from_address=False,
            limit=limit,
            stop_timestamp=stop_timestamp,
        ),
    )
    all_transfers = outgoing + incoming
    all_transfers.sort(
        key=lambda x: x.get("_parsed_timestamp") or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    # Deduplicate by hash + uniqueId where available
    seen: Dict[str, Dict[str, Any]] = {}
    for transfer in all_transfers:
        unique_id = transfer.get("uniqueId")
        key = f"{transfer.get('hash')}::{unique_id}"
        if key in seen:
            continue
        seen[key] = transfer
    sorted_transfers = list(seen.values())
    sorted_transfers.sort(
        key=lambda x: x.get("_parsed_timestamp") or datetime.fromtimestamp(0, tz=timezone.utc),
        reverse=True,
    )
    if limit:
        sorted_transfers = sorted_transfers[:limit]
    if stop_timestamp:
        sorted_transfers = [
            tx for tx in sorted_transfers if tx.get("_parsed_timestamp") and tx["_parsed_timestamp"] >= stop_timestamp
        ]
    return sorted_transfers
