#!/usr/bin/env python3
"""Stage 2 (offline): turn the local extract into the settled-value ladder.

Reads data/extract/*.jsonl.gz, writes data/blocks.csv (one row per block) and
data/daily.csv (one row per complete UTC day, BTC and USD).

Ladder, per output of every non-coinbase transaction (OP_RETURN never counted):
  raw    every output
  base   minus obvious change: output script equals the script of any input
         of the same transaction
  early  minus early spend: a base output spent again less than one hour
         later, by block timestamp
  adj_k  minus assumed change: among the base outputs of a transaction, when at
         least one is a round amount, the non-round ones are treated as change.
         "Round" = divisible by ROUND[k] sats; all five precisions are computed.
"""
import argparse
import csv
import gzip
import json
import multiprocessing as mp
import os
import sys
import time
import urllib.request
from collections import deque

HERE = os.path.dirname(os.path.abspath(__file__))
EXTRACT = os.path.join(HERE, "data", "extract")
REF_DEPTH = 36                      # must match fetch.py
EARLY_SECONDS = 3600
ROUND = [10**3, 10**4, 10**5, 10**6, 10**7]   # sats
NK = len(ROUND)
NBUCKET = 11                        # base output size histogram, by decade of sats

BLOCK_COLS = (["h", "t", "final", "ntx", "cb", "fee", "raw", "base", "e"]
              + [f"a{k}" for k in range(NK)] + [f"ea{k}" for k in range(NK)]
              + ["n_out", "n_base", "n_txbase"]
              + [f"hc{i}" for i in range(NBUCKET)] + [f"hs{i}" for i in range(NBUCKET)])


def roundness(v):
    r = 0
    for R in ROUND:
        if v % R:
            break
        r += 1
    return r


class Rec:
    __slots__ = ("h", "t", "ntx", "cb", "fee", "raw", "base", "e", "a", "ea",
                 "n_out", "n_base", "n_txbase", "hc", "hs", "txids")

    def row(self, final):
        return ([self.h, self.t, int(final), self.ntx, self.cb, self.fee, self.raw,
                 self.base, self.e] + self.a + self.ea
                + [self.n_out, self.n_base, self.n_txbase] + self.hc + self.hs)


def read_chunk(cid):
    with gzip.open(os.path.join(EXTRACT, f"{cid:06d}.jsonl.gz"), "rt") as f:
        for line in f:
            yield json.loads(line)


def segment(args):
    """Process own chunks, then one lookahead chunk for spends only."""
    own, lookahead = args
    recent = {}          # txid16 -> (rec, [None | (value, amask)] per vout)
    pending = deque()
    rows = []
    times = deque(maxlen=REF_DEPTH + 1)
    deep_hours = 0       # blocks where REF_DEPTH blocks fit inside one hour

    def spend(b):
        t = b["t"]
        for tx in b["tx"]:
            for i in tx[1]:
                if len(i) == 5:
                    prev = recent.get(i[3])
                    if prev is not None:
                        rec, outs = prev
                        st = outs[i[4]]
                        if st is not None and t - rec.t < EARLY_SECONDS:
                            v, amask = st
                            rec.e += v
                            k = 0
                            while amask:
                                if amask & 1:
                                    rec.ea[k] += v
                                amask >>= 1
                                k += 1

    def flush(upto_h, final):
        while pending and pending[0].h <= upto_h:
            rec = pending.popleft()
            for txid in rec.txids:
                del recent[txid]
            rec.txids = None
            rows.append(rec.row(final))

    for cid in own:
        for b in read_chunk(cid):
            t = b["t"]
            if len(times) == times.maxlen and t - times[0] < EARLY_SECONDS:
                deep_hours += 1
            times.append(t)
            rec = Rec()
            rec.h, rec.t, rec.cb = b["h"], t, b["cb"]
            rec.ntx = len(b["tx"]) + 1
            rec.fee = rec.raw = rec.base = rec.e = 0
            rec.a = [0] * NK
            rec.ea = [0] * NK
            rec.n_out = rec.n_base = rec.n_txbase = 0
            rec.hc = [0] * NBUCKET
            rec.hs = [0] * NBUCKET
            rec.txids = []
            for txid, ins, outs in b["tx"]:
                in_keys = set()
                in_val = 0
                for i in ins:
                    in_keys.add(i[0])
                    in_val += i[1]
                    if len(i) == 5:
                        prev = recent.get(i[3])
                        if prev is not None:
                            prec, pouts = prev
                            st = pouts[i[4]]
                            if st is not None and t - prec.t < EARLY_SECONDS:
                                v, amask = st
                                prec.e += v
                                k = 0
                                while amask:
                                    if amask & 1:
                                        prec.ea[k] += v
                                    amask >>= 1
                                    k += 1
                out_val = 0
                base_idx = []
                max_r = 0
                rs = []
                for n, o in enumerate(outs):
                    v = o[1]
                    out_val += v
                    if o[2] == "n":
                        continue
                    rec.raw += v
                    rec.n_out += 1
                    if o[0] in in_keys:
                        continue
                    r = roundness(v) if v else 0
                    if r > max_r:
                        max_r = r
                    base_idx.append(n)
                    rs.append(r)
                rec.fee += in_val - out_val
                state = [None] * len(outs)
                if base_idx:
                    rec.n_txbase += 1
                    for n, r in zip(base_idx, rs):
                        v = outs[n][1]
                        rec.base += v
                        rec.n_base += 1
                        bkt = min(len(str(v)) - 1, NBUCKET - 1)
                        rec.hc[bkt] += 1
                        rec.hs[bkt] += v
                        # assumed change at precision k  <=>  r <= k < max_r
                        amask = 0
                        for k in range(r, max_r):
                            amask |= 1 << k
                            rec.a[k] += v
                        state[n] = (v, amask)
                recent[txid] = (rec, state)
                rec.txids.append(txid)
            pending.append(rec)
            flush(rec.h - REF_DEPTH - 1, True)
    if lookahead is not None:
        for b in read_chunk(lookahead):
            spend(b)
        flush(10**9, True)
    else:
        flush(10**9, False)   # tail of the data: early spends not yet all seen
    return rows, deep_hours


def load_prices(dates):
    """Coin Metrics community reference rate, daily. Cached in data/prices.csv."""
    path = os.path.join(HERE, "data", "prices.csv")
    prices = {}
    if os.path.exists(path):
        with open(path) as f:
            prices = {r["date"]: float(r["price_usd"]) for r in csv.DictReader(f)}
    missing = [d for d in dates if d not in prices]
    if missing:
        url = ("https://community-api.coinmetrics.io/v4/timeseries/asset-metrics"
               f"?assets=btc&metrics=PriceUSD&frequency=1d&page_size=10000"
               f"&start_time={min(missing)}&end_time={max(missing)}")
        while url:
            req = urllib.request.Request(url, headers={"User-Agent": "bitcoin-moe"})
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.load(r)
            for it in d["data"]:
                if it.get("PriceUSD"):
                    prices[it["time"][:10]] = float(it["PriceUSD"])
            url = d.get("next_page_url")
        with open(path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["date", "price_usd"])
            for d in sorted(prices):
                w.writerow([d, prices[d]])
    return prices


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    ap.add_argument("--from-height", type=int, default=0,
                    help="ignore extract files below this height")
    ap.add_argument("--to-height", type=int, default=10**9,
                    help="ignore extract files above this height")
    args = ap.parse_args()

    cids = sorted(int(f[:6]) for f in os.listdir(EXTRACT) if f.endswith(".jsonl.gz"))
    cids = [c for c in cids if args.from_height <= c * 100 <= args.to_height]
    if not cids:
        sys.exit("no extract files")
    gaps = [c for a, c in zip(cids, cids[1:]) if c != a + 1]
    if gaps:
        sys.exit(f"extract has gaps before chunks {gaps[:10]}; run fetch.py first")

    per = max(1, -(-len(cids) // (args.workers * 4)))
    jobs = []
    for i in range(0, len(cids), per):
        own = cids[i:i + per]
        nxt = cids[i + per] if i + per < len(cids) else None
        jobs.append((own, nxt))
    t0 = time.time()
    rows = []
    deep = 0
    with mp.Pool(args.workers) as pool:
        for r, d in pool.imap(segment, jobs):
            rows.extend(r)
            deep += d
    rows.sort(key=lambda r: r[0])
    for a, b in zip(rows, rows[1:]):
        if b[0] != a[0] + 1:
            sys.exit(f"block rows not contiguous at {a[0]} -> {b[0]}")
    with open(os.path.join(HERE, "data", "blocks.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(BLOCK_COLS)
        w.writerows(rows)
    print(f"{len(rows)} blocks {rows[0][0]}..{rows[-1][0]} in {time.time() - t0:.0f}s; "
          f"blocks where {REF_DEPTH} blocks fit in one hour: {deep}")

    # daily
    C = {c: i for i, c in enumerate(BLOCK_COLS)}
    days = {}
    for r in rows:
        d = time.strftime("%Y-%m-%d", time.gmtime(r[C["t"]]))
        days.setdefault(d, []).append(r)
    dates = sorted(days)
    complete = [d for d in dates[1:-1] if all(r[C["final"]] for r in days[d])]
    prices = load_prices(complete)
    out_cols = (["date", "first_height", "last_height", "blocks", "price_usd", "tx_count",
                 "transfer_count", "raw_btc", "base_btc", "early_btc"]
                + [f"adj{k}_btc" for k in range(NK)]
                + ["raw_usd", "base_usd", "early_usd"] + [f"adj{k}_usd" for k in range(NK)])
    with open(os.path.join(HERE, "data", "daily.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(out_cols)
        for d in complete:
            if d not in prices:
                sys.exit(f"no price for {d}")
            rs = days[d]
            s = lambda c: sum(r[C[c]] for r in rs)  # noqa: E731
            raw, base, e = s("raw"), s("base"), s("e")
            btc = [raw, base, base - e] + [base - e - (s(f"a{k}") - s(f"ea{k}"))
                                           for k in range(NK)]
            btc = [x / 1e8 for x in btc]
            p = prices[d]
            w.writerow([d, rs[0][C["h"]], rs[-1][C["h"]], len(rs), p, s("ntx"), s("n_base")]
                       + [round(x, 8) for x in btc] + [round(x * p, 2) for x in btc])
    print(f"{len(complete)} complete days {complete[0]}..{complete[-1]} -> data/daily.csv")


if __name__ == "__main__":
    main()
