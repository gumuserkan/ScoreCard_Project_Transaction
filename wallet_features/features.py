"""Feature extraction logic."""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

from wallet_features.clients.alchemy import AlchemyClient, gather_wallet_transfers
from wallet_features.pricing import PriceService
from wallet_features.utils import TimeWindow, utc_now

LOGGER = logging.getLogger(__name__)

WINDOWS = [
    TimeWindow("1M", 30),
    TimeWindow("3M", 90),
    TimeWindow("6M", 180),
    TimeWindow("12M", 365),
]

KNOWN_DEX_ROUTERS = {
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",  # Uniswap V2
    "0xe592427a0aece92de3edee1f18e0157c05861564",  # Uniswap V3 router
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45",  # Uniswap V3 periphery
    "0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F".lower(),  # Sushi
}


@dataclass
class TransferEvent:
    """Normalized transfer event."""

    tx_hash: str
    unique_id: str
    timestamp: datetime
    category: str
    asset: Optional[str]
    raw_value: Optional[str]
    value: Optional[float]
    raw_contract: Dict[str, Any]
    from_address: Optional[str]
    to_address: Optional[str]
    direction: str
    raw: Dict[str, Any]

    @property
    def contract_address(self) -> Optional[str]:
        address = self.raw_contract.get("address")
        if address:
            return str(address).lower()
        contract = self.raw.get("rawContract", {}).get("address")
        if contract:
            return str(contract).lower()
        return None


class FeatureCalculator:
    """Coordinates fetching and computation of wallet features."""

    def __init__(
        self,
        client: AlchemyClient,
        prices: PriceService,
        network: str = "eth-mainnet",
        *,
        include_gas_fees: bool = True,
    ):
        self.client = client
        self.prices = prices
        self.network = network
        self.include_gas_fees = include_gas_fees
        self._metadata_cache: Dict[str, Dict[str, Any]] = {}
        self._receipt_cache: Dict[str, Dict[str, Any]] = {}
        self._transactions_cache: Dict[str, List[TransferEvent]] = {}

    async def get_token_metadata(self, contract_address: str) -> Optional[Dict[str, Any]]:
        if contract_address in self._metadata_cache:
            return self._metadata_cache[contract_address]
        meta = await self.client.get_token_metadata(contract_address)
        if meta:
            self._metadata_cache[contract_address] = meta
        return meta

    async def get_transaction_receipt(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        if tx_hash in self._receipt_cache:
            return self._receipt_cache[tx_hash]
        receipt = await self.client.get_transaction_receipt(tx_hash)
        if receipt:
            self._receipt_cache[tx_hash] = receipt
        return receipt

    async def fetch_wallet_data(self, wallet: str) -> Tuple[List[TransferEvent], List[TransferEvent]]:
        now = utc_now()
        cutoff_year = now - timedelta(days=365)
        transfers_year_raw = await gather_wallet_transfers(
            self.client,
            wallet,
            stop_timestamp=cutoff_year,
        )
        transfers_last250_raw = await gather_wallet_transfers(
            self.client,
            wallet,
            limit=250,
        )
        events_year = [self._normalize_transfer(wallet, t) for t in transfers_year_raw]
        events_250 = [self._normalize_transfer(wallet, t) for t in transfers_last250_raw]
        return events_year, events_250

    def _normalize_transfer(self, wallet: str, transfer: Dict[str, Any]) -> TransferEvent:
        tx_hash = str(transfer.get("hash", "")).lower()
        unique_id = str(transfer.get("uniqueId") or tx_hash)
        ts = transfer.get("_parsed_timestamp")
        if not isinstance(ts, datetime):
            ts = datetime.fromtimestamp(0, tz=timezone.utc)
        category = str(transfer.get("category") or "unknown").lower()
        asset = transfer.get("asset")
        raw_contract = transfer.get("rawContract") or {}
        raw_value = raw_contract.get("value")
        value = None
        transfer_value = transfer.get("value")
        if isinstance(transfer_value, (int, float)):
            value = float(transfer_value)
        elif isinstance(transfer_value, str):
            try:
                value = float(transfer_value)
            except ValueError:
                value = None
        from_address = transfer.get("from")
        to_address = transfer.get("to")
        direction = "unknown"
        if from_address and from_address.lower() == wallet:
            direction = "out"
        elif to_address and to_address.lower() == wallet:
            direction = "in"
        return TransferEvent(
            tx_hash=tx_hash,
            unique_id=unique_id,
            timestamp=ts,
            category=category,
            asset=asset,
            raw_value=raw_value,
            value=value,
            raw_contract=raw_contract,
            from_address=from_address.lower() if isinstance(from_address, str) else None,
            to_address=to_address.lower() if isinstance(to_address, str) else None,
            direction=direction,
            raw=transfer,
        )

    async def compute_wallet_features(self, wallet: str) -> Dict[str, Any]:
        events_year, events_250 = await self.fetch_wallet_data(wallet)
        combined_events = self._combine_events(events_year, events_250)
        self._transactions_cache[wallet] = combined_events
        if not events_year and not events_250:
            return self._empty_features(wallet)
        events_year.sort(key=lambda e: e.timestamp, reverse=True)
        events_250.sort(key=lambda e: e.timestamp, reverse=True)
        reference = utc_now()
        windows_cutoff = {w.label: reference - timedelta(days=w.days) for w in WINDOWS}
        counts: Dict[str, int] = {f"Total Tx Count ({w.label})": 0 for w in WINDOWS}
        volumes: Dict[str, float] = {f"Total Tx Volume ({w.label})": 0.0 for w in WINDOWS}
        monthly_counts = 0
        monthly_volume = 0.0
        # Group events by transaction hash
        tx_events: Dict[str, List[TransferEvent]] = defaultdict(list)
        for ev in events_year:
            tx_events[ev.tx_hash].append(ev)
        usd_cache: Dict[str, float] = {}

        async def event_usd(ev: TransferEvent) -> float:
            if ev.unique_id not in usd_cache:
                value = await self._event_usd_value(ev)
                usd_cache[ev.unique_id] = float(value or 0.0)
            return usd_cache[ev.unique_id]

        for window in WINDOWS:
            cutoff = windows_cutoff[window.label]
            tx_hashes_in_window: Set[str] = set()
            for ev in events_year:
                if ev.timestamp < cutoff:
                    continue
                tx_hashes_in_window.add(ev.tx_hash)
                volumes[f"Total Tx Volume ({window.label})"] += await event_usd(ev)
            counts[f"Total Tx Count ({window.label})"] = len(tx_hashes_in_window)
            if window.label == "12M":
                monthly_counts = len(tx_hashes_in_window)
                monthly_volume = volumes[f"Total Tx Volume ({window.label})"]
        months = 12
        monthly_count_avg = monthly_counts / months if months else 0
        monthly_volume_avg = monthly_volume / months if months else 0
        last_tx = events_year[0] if events_year else (events_250[0] if events_250 else None)
        second_last = events_year[1] if len(events_year) > 1 else (events_250[1] if len(events_250) > 1 else None)
        last_tx_date = last_tx.timestamp.isoformat() if last_tx else ""
        time_between_hours = ""
        if last_tx and second_last:
            delta = last_tx.timestamp - second_last.timestamp
            time_between_hours = f"{delta.total_seconds() / 3600:.2f}"
        categories = sorted({ev.category.upper() for ev in events_250 if ev.category})
        types_year = self._classify_transactions(wallet, tx_events)
        types_recent = self._classify_events(events_250, wallet)
        tx_types = sorted(types_year.union(types_recent))
        gas_fee_usd = 0.0
        if self.include_gas_fees:
            gas_fee_usd = await self._total_gas_fee(wallet, tx_events, reference)
        features = {
            "Wallet": wallet,
            "Monthly Tx Count Avg (12M)": round(monthly_count_avg, 4),
            "Monthly Tx Volume Avg (12M)": round(monthly_volume_avg, 4),
            "Last Transaction Date": last_tx_date,
            "Time Between Last 2 Transactions (hours)": time_between_hours,
            "Token Categories (Last 250 Tx)": ",".join(categories),
            "Tx Types (Last 250 Tx)": ",".join(sorted(tx_types)),
            "Total Gas Fee (USD)": round(gas_fee_usd, 2),
            "error": "",
        }
        for window in WINDOWS:
            features[f"Total Tx Count ({window.label})"] = counts[f"Total Tx Count ({window.label})"]
            features[f"Total Tx Volume ({window.label})"] = round(
                volumes[f"Total Tx Volume ({window.label})"], 2
            )
        return features

    def get_wallet_transactions(self, wallet: str) -> List[TransferEvent]:
        """Return cached transactions for a wallet computed during feature extraction."""

        return list(self._transactions_cache.get(wallet, []))

    def _combine_events(
        self,
        events_year: List[TransferEvent],
        events_recent: List[TransferEvent],
    ) -> List[TransferEvent]:
        """Merge yearly and recent transfer events into a de-duplicated list."""

        seen: Dict[str, TransferEvent] = {}
        for event in events_year + events_recent:
            key = event.unique_id or f"{event.tx_hash}:{event.timestamp.isoformat()}"
            seen[key] = event
        merged = list(seen.values())
        merged.sort(key=lambda ev: ev.timestamp, reverse=True)
        return merged

    async def _event_usd_value(self, event: TransferEvent) -> Optional[float]:
        if event.value is not None:
            amount = event.value
        else:
            amount = await self._decode_value(event)
        if amount is None or amount == 0:
            return 0.0
        price = await self.prices.get_price(
            contract_address=event.contract_address,
            network=self.network,
            timestamp=event.timestamp,
        )
        if not price:
            return None
        return amount * price.usd

    async def _decode_value(self, event: TransferEvent) -> Optional[float]:
        raw_value = event.raw_value
        if not raw_value:
            return None
        try:
            value_int = int(raw_value, 16)
        except ValueError:
            return None
        decimals = event.raw_contract.get("decimals")
        if decimals is None and event.contract_address:
            metadata = await self.get_token_metadata(event.contract_address)
            if metadata:
                decimals = metadata.get("decimals")
        try:
            decimals_int = int(decimals) if decimals is not None else 18
        except ValueError:
            decimals_int = 18
        return value_int / (10**decimals_int)

    def _classify_events(self, events: Iterable[TransferEvent], wallet: str) -> Set[str]:
        types: Set[str] = set()
        tx_map: Dict[str, List[TransferEvent]] = defaultdict(list)
        for ev in events:
            tx_map[ev.tx_hash].append(ev)
        for tx_hash, tx_events in tx_map.items():
            types.add(self._classify_single(wallet, tx_events))
        return types

    def _classify_transactions(self, wallet: str, tx_events: Dict[str, List[TransferEvent]]) -> Set[str]:
        types: Set[str] = set()
        for events in tx_events.values():
            types.add(self._classify_single(wallet, events))
        return types

    def _classify_single(self, wallet: str, events: List[TransferEvent]) -> str:
        wallet = wallet.lower()
        categories = {ev.category for ev in events}
        contracts = {ev.contract_address for ev in events if ev.contract_address}
        if any(ev.category in {"erc721", "erc1155"} and ev.to_address == wallet for ev in events):
            return "MINT"
        if any(ev.category in {"erc721", "erc1155"} and ev.from_address == wallet for ev in events):
            return "BURN"
        if any(ev.to_address in KNOWN_DEX_ROUTERS for ev in events if ev.to_address):
            return "SWAP"
        if any(ev.from_address == wallet and ev.to_address == wallet for ev in events):
            return "TRANSFER"
        incoming = any(ev.to_address == wallet for ev in events)
        outgoing = any(ev.from_address == wallet for ev in events)
        if incoming and outgoing:
            return "SWAP"
        if "external" in categories or "internal" in categories:
            return "TRANSFER"
        if "erc20" in categories:
            if outgoing:
                return "TRANSFER"
            if incoming:
                return "TRANSFER"
        if contracts:
            return "CONTRACT_INTERACTION"
        return "UNKNOWN"

    async def _total_gas_fee(
        self,
        wallet: str,
        tx_events: Dict[str, List[TransferEvent]],
        reference: datetime,
    ) -> float:
        total_usd = 0.0
        wallet_lower = wallet.lower()
        for tx_hash, events in tx_events.items():
            # Gas is paid by sender
            sender = None
            timestamp = None
            for ev in events:
                if ev.from_address:
                    sender = ev.from_address
                if not timestamp or ev.timestamp > timestamp:
                    timestamp = ev.timestamp
            if sender != wallet_lower or not tx_hash:
                continue
            receipt = await self.get_transaction_receipt(tx_hash)
            if not receipt:
                continue
            gas_used_hex = receipt.get("gasUsed")
            eff_price_hex = receipt.get("effectiveGasPrice") or receipt.get("gasPrice")
            if not gas_used_hex or not eff_price_hex:
                continue
            try:
                gas_used = int(gas_used_hex, 16)
                effective_price = int(eff_price_hex, 16)
            except ValueError:
                continue
            eth_spent = gas_used * effective_price / 1e18
            if timestamp is None:
                timestamp = reference
            price = await self.prices.get_price(contract_address=None, network=self.network, timestamp=timestamp)
            if not price:
                continue
            total_usd += eth_spent * price.usd
        return total_usd

    def _empty_features(self, wallet: str) -> Dict[str, Any]:
        base = {
            "Wallet": wallet,
            "Monthly Tx Count Avg (12M)": 0.0,
            "Monthly Tx Volume Avg (12M)": 0.0,
            "Last Transaction Date": "",
            "Time Between Last 2 Transactions (hours)": "",
            "Token Categories (Last 250 Tx)": "",
            "Tx Types (Last 250 Tx)": "",
            "Total Gas Fee (USD)": 0.0,
            "error": "",
        }
        for window in WINDOWS:
            base[f"Total Tx Count ({window.label})"] = 0
            base[f"Total Tx Volume ({window.label})"] = 0.0
        return base


async def compute_wallets_features(
    wallets: Iterable[str],
    calculator: FeatureCalculator,
    *,
    concurrency: int = 10,
) -> Dict[str, Dict[str, Any]]:
    """Compute features for multiple wallets concurrently."""

    results: Dict[str, Dict[str, Any]] = {}
    errors: Dict[str, str] = {}

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def worker(wallet: str) -> None:
        async with semaphore:
            try:
                results[wallet] = await calculator.compute_wallet_features(wallet)
            except Exception as exc:  # pragma: no cover - runtime protective clause
                LOGGER.exception("Failed to compute features for %s", wallet)
                errors[wallet] = str(exc)

    await asyncio.gather(*(worker(wallet) for wallet in wallets))
    for wallet, error in errors.items():
        base = calculator._empty_features(wallet)
        base["error"] = error
        results[wallet] = base
    return results
