#!/usr/bin/env python3
"""Sum the ladder in data/daily.csv over a date range (inclusive, UTC days)."""
import argparse
import csv
import datetime as dt
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
RUNGS = [("raw", "Raw output value"),
         ("base", "Minus obvious change"),
         ("early", "Minus early spend (< 1 h)"),
         ("adj4", "Minus assumed change, round = 0.1 BTC"),
         ("adj3", "Minus assumed change, round = 0.01 BTC"),
         ("adj2", "Minus assumed change, round = 0.001 BTC"),
         ("adj1", "Minus assumed change, round = 0.0001 BTC"),
         ("adj0", "Minus assumed change, round = 0.00001 BTC")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("start")
    ap.add_argument("end")
    a = ap.parse_args()
    rows = {r["date"]: r for r in csv.DictReader(open(os.path.join(HERE, "data", "daily.csv")))}
    d0, d1 = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
    days = [(d0 + dt.timedelta(n)).isoformat() for n in range((d1 - d0).days + 1)]
    missing = [d for d in days if d not in rows]
    if missing:
        sys.exit(f"{len(missing)} days missing from daily.csv, first {missing[0]}, last {missing[-1]}")
    sel = [rows[d] for d in days]
    n = len(sel)
    raw = sum(float(r["raw_usd"]) for r in sel)
    print(f"{a.start} .. {a.end}: {n} days, blocks {sel[0]['first_height']}..{sel[-1]['last_height']}, "
          f"avg price ${sum(float(r['price_usd']) for r in sel) / n:,.0f}, "
          f"{sum(int(r['tx_count']) for r in sel):,} transactions")
    print(f"{'rung':44} {'USD total':>10} {'USD/day':>9} {'BTC/day':>10} {'of raw':>7}")
    for key, label in RUNGS:
        usd = sum(float(r[key + "_usd"]) for r in sel)
        btc = sum(float(r[key + "_btc"]) for r in sel)
        print(f"{label:44} {usd / 1e12:9.2f}T {usd / n / 1e9:8.2f}B {btc / n:10,.0f} {usd / raw * 100:6.1f}%")


if __name__ == "__main__":
    main()
