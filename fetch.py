#!/usr/bin/env python3
"""Stage 1: pull blocks from a Bitcoin Core node and save a compact local extract.

The node only answers RPC (getblockhash, getblock verbosity 3) through an SSH
tunnel. Everything else runs on this machine. Output is one gzip file per 100
blocks under data/extract/, which stage 2 (compute.py) reads fully offline.

Extract line (one JSON object per block):
  h, t, mt, id     height, block time, median time, block hash
  cb               coinbase output value in sats
  tx               list of [txid16, ins, outs] for every non-coinbase tx
    ins            [script_key, value_sats, prevout_height] and, when the prevout
                   is at most REF_DEPTH blocks old, + [prev_txid16, prev_vout]
    outs           [script_key, value_sats, type_code]
  script_key       first 8 bytes of blake2b(scriptPubKey), hex
"""
import argparse
import base64
import gzip
import hashlib
import http.client
import json
import multiprocessing as mp
import os
import subprocess
import sys
import time

CHUNK = 100          # blocks per extract file
REF_DEPTH = 36       # keep the prevout reference for inputs this recent
CONFIRMATIONS = 6    # never fetch closer to the tip than this
BASE_PORT = 18340

TYPE_CODES = {
    "pubkeyhash": "k", "scripthash": "s", "witness_v0_keyhash": "w",
    "witness_v0_scripthash": "W", "witness_v1_taproot": "t", "nulldata": "n",
    "pubkey": "p", "multisig": "m", "nonstandard": "x", "witness_unknown": "u",
    "anchor": "a",
}

HERE = os.path.dirname(os.path.abspath(__file__))


def load_env():
    env = {}
    with open(os.path.join(HERE, ".env")) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k] = v
    for k in ("NODE_SSH", "RPC_USER", "RPC_PASSWORD"):
        if k not in env:
            sys.exit(f".env is missing {k}")
    return env


class Node:
    """JSON-RPC over an SSH tunnel owned by this process."""

    def __init__(self, env, slot):
        self.env = env
        self.port = BASE_PORT + slot
        self.sock = os.path.join(HERE, "data", f".tunnel-{slot}.sock")
        auth = f"{env['RPC_USER']}:{env['RPC_PASSWORD']}".encode()
        self.headers = {
            "Authorization": "Basic " + base64.b64encode(auth).decode(),
            "Content-Type": "application/json",
        }
        self.conn = None
        self.open_tunnel()

    def open_tunnel(self):
        self.close_tunnel()
        subprocess.run(
            ["ssh", "-C", "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
             "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4",
             "-M", "-S", self.sock, "-f", "-N",
             "-L", f"{self.port}:127.0.0.1:{self.env.get('RPC_PORT', '8332')}",
             self.env["NODE_SSH"]],
            check=True, timeout=60)
        time.sleep(0.5)
        self.conn = None

    def close_tunnel(self):
        if os.path.exists(self.sock):
            subprocess.run(["ssh", "-S", self.sock, "-O", "exit", "x"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                os.unlink(self.sock)
            except FileNotFoundError:
                pass

    def _post(self, body):
        if self.conn is None:
            self.conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=180)
        self.conn.request("POST", "/", body=body, headers=self.headers)
        resp = self.conn.getresponse()
        data = resp.read()
        if resp.status != 200:
            raise RuntimeError(f"RPC HTTP {resp.status}: {data[:200]!r}")
        return json.loads(data)

    def call(self, payload):
        """payload: one request dict or a list (batch). Retries with a fresh tunnel."""
        body = json.dumps(payload)
        delay = 2
        for attempt in range(8):
            try:
                out = self._post(body)
                items = out if isinstance(out, list) else [out]
                for it in items:
                    if it.get("error"):
                        raise RuntimeError(f"RPC error: {it['error']}")
                return out
            except Exception as e:  # noqa: BLE001 - retried, then raised
                last = e
                try:
                    if self.conn:
                        self.conn.close()
                except Exception:  # noqa: BLE001
                    pass
                self.conn = None
                time.sleep(delay)
                delay = min(delay * 2, 60)
                try:
                    self.open_tunnel()
                except Exception as e2:  # noqa: BLE001
                    last = e2
        raise RuntimeError(f"RPC failed after retries: {last}")

    def rpc(self, method, *params):
        return self.call({"jsonrpc": "1.0", "id": 0, "method": method,
                          "params": list(params)})["result"]


def skey(spk_hex):
    return hashlib.blake2b(bytes.fromhex(spk_hex), digest_size=8).hexdigest()


def sats(v):
    return round(v * 1e8)


def extract_block(b):
    h = b["height"]
    txs = b["tx"]
    cb = sum(sats(o["value"]) for o in txs[0]["vout"])
    out_txs = []
    for tx in txs[1:]:
        ins = []
        for vin in tx["vin"]:
            p = vin["prevout"]
            row = [skey(p["scriptPubKey"]["hex"]), sats(p["value"]), p["height"]]
            if h - p["height"] <= REF_DEPTH:
                row += [vin["txid"][:16], vin["vout"]]
            ins.append(row)
        outs = []
        for o in tx["vout"]:
            spk = o["scriptPubKey"]
            code = TYPE_CODES.get(spk["type"], "?")
            outs.append(["" if code == "n" else skey(spk["hex"]), sats(o["value"]), code])
        out_txs.append([tx["txid"][:16], ins, outs])
    if len(txs) != b["nTx"]:
        raise RuntimeError(f"block {h}: got {len(txs)} txs, header says {b['nTx']}")
    return {"h": h, "t": b["time"], "mt": b["mediantime"], "id": b["hash"],
            "cb": cb, "tx": out_txs}


_node = None
_outdir = None


def worker_init(env, counter, outdir):
    global _node, _outdir
    with counter.get_lock():
        slot = counter.value
        counter.value += 1
    _node = Node(env, slot)
    _outdir = outdir


def chunk_path(outdir, cid):
    return os.path.join(outdir, f"{cid:06d}.jsonl.gz")


def do_chunk(cid):
    t0 = time.time()
    heights = range(cid * CHUNK, (cid + 1) * CHUNK)
    hashes = _node.call([{"jsonrpc": "1.0", "id": h, "method": "getblockhash",
                          "params": [h]} for h in heights])
    by_h = {it["id"]: it["result"] for it in hashes}
    path = chunk_path(_outdir, cid)
    tmp = path + f".tmp{os.getpid()}"
    with gzip.open(tmp, "wt", compresslevel=6) as f:
        for h in heights:
            b = _node.rpc("getblock", by_h[h], 3)
            if b["height"] != h:
                raise RuntimeError(f"asked for {h}, got {b['height']}")
            f.write(json.dumps(extract_block(b), separators=(",", ":")) + "\n")
    os.replace(tmp, path)
    return cid, time.time() - t0, os.path.getsize(path)


def worker_exit(_):
    if _node:
        _node.close_tunnel()


def height_at(node, ts, tip):
    """First height whose block time is >= ts."""
    lo, hi = 0, tip
    while lo < hi:
        mid = (lo + hi) // 2
        t = node.rpc("getblockheader", node.rpc("getblockhash", mid))["time"]
        if t < ts:
            lo = mid + 1
        else:
            hi = mid
    return lo


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-date", help="UTC date YYYY-MM-DD (start of day)")
    ap.add_argument("--to-date", help="UTC date YYYY-MM-DD (end of day); default = tip")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--newest-first", action="store_true")
    ap.add_argument("--out", default=os.path.join(HERE, "data", "extract"))
    args = ap.parse_args()

    env = load_env()
    os.makedirs(args.out, exist_ok=True)
    import calendar
    def ts(d, end=False):
        t = calendar.timegm(time.strptime(d, "%Y-%m-%d"))
        return t + 86400 if end else t

    ctl = Node(env, 99)
    try:
        tip = ctl.rpc("getblockcount")
        safe = tip - CONFIRMATIONS
        # 2 h of slack on each side: block times are not monotonic
        first = height_at(ctl, ts(args.from_date) - 7200, safe)
        last = height_at(ctl, ts(args.to_date, True) + 7200, safe) if args.to_date else safe
    finally:
        ctl.close_tunnel()
    c0 = first // CHUNK
    c1 = (last + 1) // CHUNK  # exclusive: only complete chunks
    todo = [c for c in range(c0, c1) if not os.path.exists(chunk_path(args.out, c))]
    print(f"tip {tip}, heights {c0 * CHUNK}..{c1 * CHUNK - 1}, "
          f"{c1 - c0} chunks, {len(todo)} to fetch, {args.workers} workers", flush=True)
    if not todo:
        return
    if args.newest_first:
        todo.reverse()
    counter = mp.Value("i", 0)
    t0 = time.time()
    done = 0
    with mp.Pool(args.workers, worker_init, (env, counter, args.out)) as pool:
        try:
            for cid, secs, size in pool.imap_unordered(do_chunk, todo):
                done += 1
                rate = done * CHUNK / (time.time() - t0)
                eta = (len(todo) - done) * CHUNK / rate / 3600
                print(f"{time.strftime('%H:%M:%S')} chunk {cid} {secs:.0f}s "
                      f"{size / 1e6:.1f}MB  {done}/{len(todo)}  "
                      f"{rate:.2f} blk/s  eta {eta:.1f}h", flush=True)
        finally:
            pool.map(worker_exit, range(args.workers * 4))
    print("fetch complete", flush=True)


if __name__ == "__main__":
    main()
