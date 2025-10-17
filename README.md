# Wallet Features CLI

An asynchronous, production-ready CLI for extracting Ethereum wallet transaction features via Alchemy. The tool ingests wallet addresses, queries Alchemy for transaction and transfer data, enriches with historical token prices, and emits a CSV summary per wallet.

## Features

- Supports CSV/text/Excel input files or direct `--wallets` comma-separated lists
- ENS resolution, wallet validation, and graceful handling of invalid inputs
- Concurrent asynchronous calls using `aiohttp` with retry/backoff
- Aggregated transaction counts, volumes, gas fees, token categories, and transaction type inference
- Historical USD pricing via CoinGecko with caching and ETH fallback
- Structured logging with optional verbose output
- Unit tests for computation logic via `pytest`

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Usage

### Configuration

1. Copy `.env.example` to `.env` and add your secrets:

   ```bash
   cp .env.example .env
   echo "ALCHEMY_API_KEY=your_key_here" >> .env
   ```

   The CLI automatically loads the `.env` file on startup. Environment variables that are already set in the shell are not overridden.

2. Create a `data/wallets.xlsx` file that contains the wallet addresses you want to process. The first row must include a column named `address`, and each subsequent row should include a wallet address beneath that header. Example:

   | address                        |
   | ----------------------------- |
   | 0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae |
   | vitalik.eth                    |

3. (Optional) You can still provide CSV or newline-delimited text files if you prefer, or pass addresses directly with `--wallets`.

### Usage

Set your Alchemy API key in the environment or `.env` file, or pass it via CLI.

```bash
wallet-features --input data/wallets.xlsx --output features.csv --network eth-mainnet --concurrency 8
```

If you omit `--input` and a `data/wallets.xlsx` file exists, it will be used automatically.

Or pass wallets directly:

```bash
wallet-features --wallets 0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae,0xddbd1b7b8d1d6f7a5b7d6400bfb9ffbff7c8db8f --output features.csv
```

Use `--verbose` for debug-level logs.

### Sample CSV Output

```
Wallet,Total Tx Count (1M),Total Tx Count (3M),Total Tx Count (6M),Total Tx Count (12M),Monthly Tx Count Avg (12M),Total Tx Volume (1M),Total Tx Volume (3M),Total Tx Volume (6M),Total Tx Volume (12M),Monthly Tx Volume Avg (12M),Last Transaction Date,Time Between Last 2 Transactions (hours),Token Categories (Last 250 Tx),Tx Types (Last 250 Tx),Total Gas Fee (USD),error
0xde0b295669a9fd93d5f28d9ec85e40f4cb697bae,4,12,22,40,3.3333,15234.12,45012.85,80021.44,145000.75,12083.3958,2024-05-18T12:54:10+00:00,2.50,EXTERNAL,TRANSFER,125.67,
```

(The above row is illustrative.)

## Example Script

A convenience script for running the tool against two public Ethereum addresses is provided at `examples/sample_run.py`:

The example script expects an API key via `.env` or the `ALCHEMY_API_KEY` environment variable:

```bash
python examples/sample_run.py
```

This writes `sample_features.csv` in the project root.

## Running Tests

```bash
pip install -e .
pip install pytest
pytest
```

## Environment Variables

- `ALCHEMY_API_KEY`: API key used for Alchemy requests (required unless `--alchemy-key` is provided).

## Development Notes

- Retries implement exponential backoff with jitter for HTTP 429/5xx responses.
- Token metadata and price lookups are cached in-memory per process to reduce redundant requests.
- When price data for a token is unavailable, the tool falls back to the ETH/USD price at the transaction timestamp.
- The output always includes an `error` column; if an address fails to process, the error message is recorded there.
