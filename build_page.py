#!/usr/bin/env python3
"""Build the static page in docs/ (served by GitHub Pages) from data/daily.csv.

Writes docs/index.html and docs/daily.csv for one fixed period. Every number on the
page is computed here from the CSV.
"""
import argparse
import csv
import datetime as dt
import html
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_URL = "https://github.com/kingonly/bitcoin-moe"

# The round-number precision used for the last row.
STRICT = "adj0"
ROUND_BTC = {"adj0": "0.00001", "adj1": "0.0001", "adj2": "0.001", "adj3": "0.01", "adj4": "0.1"}

# Rows of the table: rung, name, one-line rule.
ROWS = [("raw", "All outputs", "Every output of every transaction."),
        ("base", "Change removed", "Outputs paid back to a sending address."),
        ("early", "Early spends removed", "Outputs spent again within an hour."),
        (STRICT, "Assumed change removed", "Non-round outputs paid alongside a round one.")]


def tn(usd):
    return f"${usd / 1e12:.2f}T"


def bn(usd):
    return f"${usd / 1e9:.1f}B"


def per_tx(usd):
    """Three significant figures: $101K, $25.8K."""
    k = usd / 1e3
    return f"${k:,.0f}K" if k >= 100 else f"${k:.1f}K"


def long_date(d):
    return f"{d.day} {d.strftime('%B')}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-09-30")
    a = ap.parse_args()
    d0, d1 = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)

    with open(os.path.join(HERE, "data", "daily.csv"), newline="") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        rows = {r["date"]: r for r in reader}
    days = [(d0 + dt.timedelta(n)).isoformat() for n in range((d1 - d0).days + 1)]
    missing = [d for d in days if d not in rows]
    if missing:
        sys.exit(f"{len(missing)} days missing from daily.csv, first {missing[0]}")
    sel = [rows[d] for d in days]
    n = len(sel)

    usd = {k: sum(float(r[k + "_usd"]) for r in sel) for k, _, _ in ROWS}
    tx = sum(int(r["tx_count"]) for r in sel)
    h0, h1 = sel[0]["first_height"], sel[-1]["last_height"]
    period_long = f"{long_date(d0)} to {long_date(d1)} {d1.year}"

    ladder = []
    for i, (k, name, rule) in enumerate(ROWS):
        ladder.append(
            f'        <tr><td><i class="sw{f" c{i}" if i else ""}"></i>{html.escape(name)}<small>{html.escape(rule)}</small></td>'
            f'<td>{tn(usd[k])}</td><td class="pd">{bn(usd[k] / n)}</td><td>{per_tx(usd[k] / tx)}</td>'
            f'<td class="pd">{usd[k] / usd["raw"] * 100:.0f}%</td></tr>')

    data = [[r["date"]] + [round(float(r[k + "_usd"]) / 1e9, 2) for k, _, _ in ROWS] for r in sel]
    # the chart plots the filtered rows only; all outputs would flatten them
    series = [{"k": i + 1, "name": name, "color": f"var(--c{i})"} for i, (_, name, _) in enumerate(ROWS) if i]

    subs = {
        "__META_DESC__": f"Value of bitcoin transaction outputs, {period_long}, before and after removing change. Computed from the chain itself.",
        "__PERIOD_LONG__": period_long,
        "__LADDER__": "\n".join(ladder),
        "__H0__": f"{int(h0):,}",
        "__H1__": f"{int(h1):,}",
        "__TX__": f"{tx:,}",
        "__ROUND__": ROUND_BTC[STRICT],
        "__REPO_URL__": REPO_URL,
        "__DATA__": json.dumps(data, separators=(",", ":")),
        "__SERIES__": json.dumps(series),
    }
    page = open(os.path.join(HERE, "page.template.html")).read()
    for k, v in subs.items():
        if k not in page:
            sys.exit(f"template is missing {k}")
        page = page.replace(k, v)
    if "__" in page:
        sys.exit(f"unreplaced token in template near: {page[page.index('__'):][:40]!r}")

    out = os.path.join(HERE, "docs")
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "index.html"), "w") as f:
        f.write(page)
    with open(os.path.join(out, "daily.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(sel)
    print(f"docs/index.html + docs/daily.csv: {n} days, blocks {h0}..{h1}, "
          + ", ".join(f"{name} {tn(usd[k])}" for k, name, _ in ROWS))


if __name__ == "__main__":
    main()
