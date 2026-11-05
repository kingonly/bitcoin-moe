# bitcoin-moe

How much value does the Bitcoin blockchain settle per day, computed from the chain itself.

Two stages. The node only answers RPC. All computation runs on your machine.

```
Bitcoin Core node ──RPC over SSH tunnel──> fetch.py ──> data/extract/*.jsonl.gz ──> compute.py ──> data/daily.csv
   (getblock verbosity 3)                  (stage 1)      (compact, local)          (stage 2, offline)
```

## Setup

Python 3.10+, standard library only. An SSH login on a box running Bitcoin Core 25+ (no pruning).

Create `.env` next to the scripts (`chmod 600 .env`):

```
NODE_SSH=user@host
RPC_USER=...
RPC_PASSWORD=...
RPC_PORT=8332
```

## Run

```
python3 fetch.py --from-date 2025-01-01              # stage 1: pull blocks, resumable
python3 compute.py                                   # stage 2: offline, about 20 s per 1,400 blocks on 8 cores
```

`fetch.py` options: `--to-date YYYY-MM-DD`, `--workers N` (default 2 parallel streams), `--newest-first`.
It stops 6 blocks below the tip and only writes complete 100-block files, so re-running it later just adds the new blocks.
Speed is about 1.8 blocks per second with 2 streams. Disk is about 23 MB per 100 blocks.

## Output

`data/daily.csv`, one row per complete UTC day (by block timestamp):

| column | meaning |
|---|---|
| `raw_btc` | every output of every non-coinbase transaction, OP_RETURN excluded |
| `base_btc` | minus obvious change: outputs paying a script that also appears among the transaction's inputs |
| `early_btc` | minus early spends: base outputs spent again less than one hour later |
| `adj0_btc` .. `adj4_btc` | minus assumed change: when a transaction has a round base output, its non-round base outputs are treated as change. Round = a multiple of 1,000 / 10,000 / 100,000 / 1,000,000 / 10,000,000 sats |
| `*_usd` | the same, times the Coin Metrics daily reference rate |
| `tx_count`, `transfer_count` | transactions, and base outputs |

`data/blocks.csv` has the same building blocks per block, plus fees and a size histogram of base outputs.

The heuristics follow Coin Metrics' paper "On-Chain Volume" (obvious change, early spend, round numbers).
Coin Metrics does not publish its round-number precision, so all five are computed.

## Checks done on 6 to 13 August 2025

| check | result |
|---|---|
| per-block output total and fees vs the node's `getblockstats` | identical on every sampled block |
| daily raw vs Blockchain.com `output-volume` | within 0.07% (they include coinbase outputs) |
| daily average vs Coin Metrics "transfer value, adjusted" ($12.96B) | `early` $13.4B, `adj4` $11.8B, `adj0` $9.4B |
