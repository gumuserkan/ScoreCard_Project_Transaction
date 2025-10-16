"""Example script for running wallet feature extraction."""
from __future__ import annotations

import asyncio
from pathlib import Path

from wallet_features.cli import async_main
from wallet_features.utils import getenv


async def main() -> None:
    api_key = getenv("ALCHEMY_API_KEY")
    if not api_key:
        raise RuntimeError("ALCHEMY_API_KEY environment variable not set")
    await async_main(
        input_file=None,
        wallets_list="0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae,0xddbd1b7b8d1d6f7a5b7d6400bfb9ffbff7c8db8f",
        output=Path("sample_features.csv"),
        alchemy_key=api_key,
        network="eth-mainnet",
        concurrency=5,
        timeout=30,
    )


if __name__ == "__main__":
    asyncio.run(main())
