#!/usr/bin/env python3
"""Byte-level differential fuzzer: pipeline/reference.py vs PIPE on tiny random CSV datasets.

Usage: python scripts/fuzz.py --pipe "<cmd>" --seeds A-B [--workdir DIR]

Per seed: writes the dataset as exact bytes (random quoting, CR, leading spaces, broken quotes,
extra fields, boundary timestamps +-1 ms / 30 min / 7 d, mixed ISO/epoch/offset timestamps),
runs reference.py and PIPE with DATA_DIR/OUTPUT_DIR set, and compares the outputs with compare.py.
On the first mismatch prints "FUZZ FAIL seed=N dir=<path>" and keeps that dataset.
Otherwise prints "FUZZ PASS <n> seeds". Exit 0 on pass, 1 on fail, 2 on bad usage.
"""
import argparse
import datetime
import json
import os
import random
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
WINDOW_START = "2026-10-01T00:00:00Z"
WINDOW_END = "2026-10-15T00:00:00Z"
BASE_MS = 1790812800000
USERS = ["u1", "u2", "U1", "u10", "u_long_identifier_00", "u_long_identifier_01", "", " u1", "u1 "]
PRODUCTS = "p1,shoes\np2,books\np3,books2\n"
CAMPAIGNS = "c_1,search\nc_2,social\nc_3,email\n"


def gen_dataset(seed):
    """Return {relative path: bytes} for one fuzz dataset."""
    R = random.Random(seed)

    def quote(s):
        r = R.random()
        if r < 0.2:
            return '"' + s + '"'
        if r < 0.23:
            return '"' + s
        if r < 0.25:
            return s + '"'
        if r < 0.27:
            return s + "\r"
        if r < 0.29:
            return " " + s
        return s

    def ts(base_ms):
        t = base_ms + R.choice([0, 1, -1, 1800000, 1800001, 604800000, 604800001, R.randint(-10**9, 10**9)])
        r = R.random()
        if r < 0.4:
            return str(t).zfill(R.choice([0, 13, 16]))
        d = datetime.datetime.fromtimestamp(t // 1000, datetime.timezone.utc)
        if r < 0.7:
            return d.strftime("%Y-%m-%dT%H:%M:%SZ")
        off = R.randint(-10, 12) * 60 + R.choice([0, 30, 59, 60])
        sign = "+" if off >= 0 else "-"
        d2 = d + datetime.timedelta(minutes=off)
        return d2.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{abs(off) // 60:02d}:{abs(off) % 60:02d}"

    events, clicks, links = [], [], []
    for _ in range(R.randint(5, 80)):
        t = BASE_MS + R.randint(-86400000, 15 * 86400000)
        ing = t + R.choice([0, 1000, 86400000, 86401000, -5000])
        ing = datetime.datetime.fromtimestamp(ing // 1000, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        fields = [
            f"e{R.randint(0, 30)}",
            R.choice(USERS),
            ts(t),
            ing,
            R.choice(["view", "cart", "purchase", "purchase", "Purchase", ""]),
            R.choice(["p1", "p2", "p3", "", "px"]),
            R.choice(["100", "0", "0250", "10000000", "10000001", "", "1.5", "-5"]),
        ]
        line = ",".join(
            (quote(x) if k != 3 else ('"' + x + '"' if R.random() < 0.2 else x)) for k, x in enumerate(fields)
        )
        if R.random() < 0.05:
            line += ",x"
        if R.random() < 0.03:
            line = ""
        events.append(line)
    for _ in range(R.randint(0, 40)):
        t = BASE_MS + R.randint(-9 * 86400000, 15 * 86400000)
        clicks.append(",".join(quote(x) for x in [
            f"k{R.randint(0, 40)}", R.choice(USERS), R.choice(["c_1", "c_2", "c_3", "C_1", ""]), ts(t),
        ]))
    for _ in range(R.randint(0, 10)):
        links.append(",".join(quote(x) for x in [R.choice(USERS), R.choice(USERS)]))

    def table(header, rows):
        return (header + "\n" + "".join(r + "\n" for r in rows)).encode()

    return {
        "manifest.json": json.dumps({"window_start": WINDOW_START, "window_end": WINDOW_END}).encode(),
        "products.csv": ("product_id,category\n" + PRODUCTS).encode(),
        "campaigns.csv": ("campaign_id,channel\n" + CAMPAIGNS).encode(),
        "events/part-0.csv": table("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents", events),
        "clicks/part-0.csv": table("click_id,user_id,campaign_id,ts", clicks),
        "links/part-0.csv": table("user_id_a,user_id_b", links),
    }


def write_dataset(data_dir, files):
    for rel, blob in files.items():
        path = data_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(blob)


def start_side(cmd, data_dir, out_dir, err_path):
    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True)
    env = dict(os.environ, DATA_DIR=str(data_dir), OUTPUT_DIR=str(out_dir))
    err = open(err_path, "wb")
    proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=err)
    return proc, err


def finish_side(proc, err):
    rc = proc.wait()
    err.close()
    return rc


def compare(ref_out, pipe_out):
    res = subprocess.run([sys.executable, "compare.py", str(ref_out), str(pipe_out)],
                         cwd=SCRIPTS, capture_output=True, text=True)
    lines = res.stdout.strip().splitlines()
    return bool(lines) and lines[-1] == "MATCH", (lines[-1] if lines else res.stderr.strip()[-200:])


def check_seed(seed, pipe_cmd, workdir):
    """Return None on match, else a reason string. Leaves the dataset dir in place on mismatch."""
    seed_dir = workdir / f"seed-{seed}"
    shutil.rmtree(seed_dir, ignore_errors=True)
    data_dir, ref_out, pipe_out = seed_dir / "input", seed_dir / "ref", seed_dir / "pipe"
    write_dataset(data_dir, gen_dataset(seed))
    ref_proc, ref_err = start_side([sys.executable, "pipeline/reference.py"], data_dir, ref_out, seed_dir / "ref.err")
    pipe_proc, pipe_err = start_side(pipe_cmd, data_dir, pipe_out, seed_dir / "pipe.err")
    rc_ref = finish_side(ref_proc, ref_err)
    rc_pipe = finish_side(pipe_proc, pipe_err)
    if rc_ref != 0:
        return f"reference.py exit {rc_ref} (see {seed_dir / 'ref.err'})"
    if rc_pipe != 0:
        return f"pipe exit {rc_pipe} (see {seed_dir / 'pipe.err'})"
    ok, last = compare(ref_out, pipe_out)
    if not ok:
        return f"compare: {last}"
    shutil.rmtree(seed_dir, ignore_errors=True)
    return None


def parse_seeds(text):
    lo, _, hi = text.partition("-")
    lo = int(lo)
    hi = int(hi) if hi else lo
    if hi < lo:
        raise ValueError(f"empty seed range {text}")
    return range(lo, hi + 1)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--pipe", required=True, help="command under test, e.g. \"python pipeline/pipeline.py\"")
    parser.add_argument("--seeds", required=True, help="A-B or a single N")
    parser.add_argument("--workdir", default=str(ROOT.parent.parent / "stress" / "fuzz"))
    args = parser.parse_args(argv)
    try:
        seeds = parse_seeds(args.seeds)
    except ValueError as exc:
        parser.error(str(exc))
    pipe_cmd = shlex.split(args.pipe, posix=(os.name != "nt"))
    workdir = Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)

    count = 0
    for seed in seeds:
        reason = check_seed(seed, pipe_cmd, workdir)
        if reason is not None:
            print(f"FUZZ FAIL seed={seed} dir={workdir / f'seed-{seed}'}")
            print(reason)
            return 1
        count += 1
    print(f"FUZZ PASS {count} seeds")
    return 0


if __name__ == "__main__":
    sys.exit(main())
