"""Command line interface for wallet feature extraction."""
from __future__ import annotations

import asyncio
import csv
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set

import typer
from rich.console import Console
from rich.logging import RichHandler

from openpyxl import Workbook

from wallet_features.clients.alchemy import AlchemyClient
from wallet_features.features import (
    FeatureCalculator,
    TransferEvent,
    compute_wallets_features,
)
from wallet_features.pricing import PriceService
from wallet_features.utils import getenv, is_wallet_address, load_env_file, load_wallets

app = typer.Typer(help="Extract Ethereum wallet features using Alchemy")
console = Console()

HEADER = [
    "Wallet",
    "Total Tx Count (1M)",
    "Total Tx Count (3M)",
    "Total Tx Count (6M)",
    "Total Tx Count (12M)",
    "Monthly Tx Count Avg (12M)",
    "Total Tx Volume (1M)",
    "Total Tx Volume (3M)",
    "Total Tx Volume (6M)",
    "Total Tx Volume (12M)",
    "Monthly Tx Volume Avg (12M)",
    "Last Transaction Date",
    "Time Between Last 2 Transactions (hours)",
    "Token Categories (Last 250 Tx)",
    "Tx Types (Last 250 Tx)",
    "Total Gas Fee (USD)",
    "error",
]

TRANSACTION_HEADER = [
    "Timestamp",
    "Transaction Hash",
    "Direction",
    "Category",
    "Asset",
    "Value",
    "Raw Value",
    "From Address",
    "To Address",
    "Unique ID",
]


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, show_path=False, enable_link_path=False)],
    )


def _sanitize_sheet_name(name: str, used: Set[str]) -> str:
    """Create an Excel-safe sheet name, ensuring uniqueness."""

    forbidden = set('[]:*?/\\')
    sanitized = "".join("_" if char in forbidden else char for char in name)
    sanitized = sanitized.strip() or "Wallet"
    base = sanitized[:31] or "Wallet"
    candidate = base
    index = 1
    while candidate in used:
        suffix = f"_{index}"
        candidate = f"{base[: 31 - len(suffix)]}{suffix}" or "Wallet"
        index += 1
    used.add(candidate)
    return candidate


def _event_to_row(event: TransferEvent) -> List[object]:
    return [
        event.timestamp.isoformat(),
        event.tx_hash,
        event.direction,
        event.category.upper() if event.category else "",
        event.asset or "",
        event.value if event.value is not None else "",
        event.raw_value or "",
        event.from_address or "",
        event.to_address or "",
        event.unique_id,
    ]


def export_transactions_to_excel(
    transactions: Dict[str, List[TransferEvent]],
    output_path: Path,
) -> None:
    """Write wallet transactions to an Excel workbook, one sheet per wallet."""

    workbook = Workbook()
    used_names: Set[str] = set()
    if not transactions:
        sheet = workbook.active
        sheet.title = "Transactions"
        sheet.append(["No transactions available"])
    else:
        default_sheet = workbook.active
        for index, (wallet, events) in enumerate(transactions.items()):
            if index == 0:
                sheet = default_sheet
                sheet.title = _sanitize_sheet_name(wallet, used_names)
            else:
                sheet = workbook.create_sheet(_sanitize_sheet_name(wallet, used_names))
            sheet.append(TRANSACTION_HEADER)
            if not events:
                sheet.append(["No transactions available"] + [""] * (len(TRANSACTION_HEADER) - 1))
                continue
            for event in events:
                sheet.append(_event_to_row(event))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(output_path)


async def resolve_wallets(client: AlchemyClient, wallets: List[str]) -> List[str]:
    resolved: List[str] = []
    for wallet in wallets:
        if wallet.endswith(".eth"):
            address = await client.resolve_ens(wallet)
            if not address:
                logging.getLogger(__name__).warning("Skipping unresolved ENS %s", wallet)
                continue
            resolved.append(address)
        elif is_wallet_address(wallet):
            resolved.append(wallet)
        else:
            logging.getLogger(__name__).warning("Skipping invalid wallet %s", wallet)
    return resolved


async def async_main(
    input_file: Optional[Path],
    wallets_list: Optional[str],
    output: Path,
    transactions_output: Path,
    alchemy_key: str,
    network: str,
    concurrency: int,
    timeout: int,
) -> None:
    wallets_raw = load_wallets(input_file, wallets_list)
    if not wallets_raw:
        console.print("[yellow]No wallets provided.[/yellow]")
        return
    async with AlchemyClient(api_key=alchemy_key, network=network, timeout=timeout) as client:
        resolved_wallets = await resolve_wallets(client, wallets_raw)
        if not resolved_wallets:
            console.print("[red]No valid wallets to process.[/red]")
            return
        async with PriceService(timeout=timeout) as prices:
            calculator = FeatureCalculator(client, prices, network=network)
            results = await compute_wallets_features(
                resolved_wallets,
                calculator,
                concurrency=concurrency,
            )
    rows = [results[w] for w in resolved_wallets if w in results]
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in HEADER})
    console.print(f"[green]Wrote features for {len(rows)} wallets to {output}[/green]")
    transactions: Dict[str, List[TransferEvent]] = {
        wallet: calculator.get_wallet_transactions(wallet)
        for wallet in resolved_wallets
        if wallet in results
    }
    export_transactions_to_excel(transactions, transactions_output)
    console.print(
        f"[green]Wrote transaction details for {len(transactions)} wallets to {transactions_output}[/green]"
    )


@app.command()
def main(
    input: Optional[Path] = typer.Option(None, "--input", help="Input file with wallet addresses"),
    wallets: Optional[str] = typer.Option(None, "--wallets", help="Comma separated wallet addresses"),
    output: Path = typer.Option(Path("wallet_features.csv"), "--output", help="Output CSV file"),
    transactions_output: Path = typer.Option(
        Path("out/wallet_transactions.xlsx"),
        "--transactions-output",
        help="Output Excel file for detailed transactions",
    ),
    alchemy_key: Optional[str] = typer.Option(None, "--alchemy-key", help="Alchemy API key"),
    network: str = typer.Option("eth-mainnet", "--network", help="Alchemy network"),
    concurrency: int = typer.Option(10, "--concurrency", min=1, help="Concurrent API calls"),
    timeout: int = typer.Option(30, "--timeout", help="HTTP timeout seconds"),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
) -> None:
    configure_logging(verbose)
    load_env_file()
    key = alchemy_key or getenv("ALCHEMY_API_KEY")
    if not key:
        raise typer.BadParameter("Alchemy API key must be provided via --alchemy-key or ALCHEMY_API_KEY env var")
    input_path = input
    if input_path is None:
        default_excel = Path("data/wallets.xlsx")
        if default_excel.exists():
            input_path = default_excel
    try:
        asyncio.run(
            async_main(
                input_file=input_path,
                wallets_list=wallets,
                output=output,
                transactions_output=transactions_output,
                alchemy_key=key,
                network=network,
                concurrency=concurrency,
                timeout=timeout,
            )
        )
    except KeyboardInterrupt:  # pragma: no cover - manual interrupt
        console.print("[red]Interrupted[/red]")


if __name__ == "__main__":
    app()
