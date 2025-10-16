"""Utility helpers for wallet features."""
from __future__ import annotations

import asyncio
import csv
import logging
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set

try:  # pragma: no cover - import guard for optional dependency
    from dateutil import parser
except ImportError:  # pragma: no cover - fallback for test environments
    class _Parser:
        @staticmethod
        def isoparse(value: str) -> datetime:
            value = value.replace("Z", "+00:00")
            return datetime.fromisoformat(value)

    parser = _Parser()  # type: ignore

LOGGER = logging.getLogger(__name__)


WALLET_REGEX = re.compile(r"^0x[a-fA-F0-9]{40}$")


def is_wallet_address(value: str) -> bool:
    """Return True if the given string is a hex Ethereum address."""

    return bool(WALLET_REGEX.fullmatch(value))


def normalize_wallet(value: str) -> str:
    """Normalize ENS or address strings."""

    value = value.strip()
    if not value:
        return value
    if value.startswith("0x"):
        return value.lower()
    return value.lower()


def load_wallets(
    input_path: Optional[Path], wallets_list: Optional[str]
) -> List[str]:
    """Load wallet addresses from CLI parameters."""

    wallets: Set[str] = set()
    if wallets_list:
        for part in wallets_list.split(","):
            part = part.strip()
            if part:
                wallets.add(normalize_wallet(part))
    if input_path:
        if not input_path.exists():
            raise FileNotFoundError(f"Input file {input_path} not found")
        if input_path.suffix.lower() == ".csv":
            with input_path.open("r", newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                for row in reader:
                    if not row:
                        continue
                    wallets.add(normalize_wallet(row[0]))
        else:
            with input_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if line.strip():
                        wallets.add(normalize_wallet(line))
    return sorted(wallets)


def utc_now() -> datetime:
    """Return timezone-aware utc now."""

    return datetime.now(timezone.utc)


def ensure_event_loop() -> asyncio.AbstractEventLoop:
    """Get or create an asyncio event loop."""

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


def async_retry(
    retries: int = 5,
    base_delay: float = 0.5,
    jitter: float = 0.2,
    exceptions: Sequence[type[BaseException]] = (Exception,),
):
    """Decorator implementing exponential backoff for async callables."""

    def decorator(func):
        async def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as exc:  # pragma: no cover - runtime only
                    attempt += 1
                    if attempt > retries:
                        raise
                    delay = base_delay * (2 ** (attempt - 1))
                    delay += random.random() * jitter
                    LOGGER.warning(
                        "Retrying %s due to %s (attempt %s/%s, sleeping %.2fs)",
                        func.__name__,
                        exc,
                        attempt,
                        retries,
                        delay,
                    )
                    await asyncio.sleep(delay)

        return wrapper

    return decorator


def month_diff(start: datetime, end: datetime) -> int:
    """Return number of calendar months between dates, at least 1."""

    months = (end.year - start.year) * 12 + (end.month - start.month)
    if end.day >= start.day:
        months += 1
    return max(months, 1)


@dataclass(frozen=True)
class TimeWindow:
    """Represents a time window boundary."""

    label: str
    days: int

    def cutoff(self, reference: datetime) -> datetime:
        return reference - timedelta(days=self.days)


def parse_iso8601(value: str) -> datetime:
    """Parse timestamp string to aware datetime."""

    dt = parser.isoparse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class AsyncLimiter:
    """Simple semaphore-based concurrency limiter for async contexts."""

    def __init__(self, limit: int):
        self._semaphore = asyncio.Semaphore(limit)

    async def __aenter__(self):
        await self._semaphore.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._semaphore.release()
        return False


def getenv(key: str, default: Optional[str] = None) -> Optional[str]:
    """Wrapper around os.getenv for easier mocking."""

    return os.getenv(key, default)
