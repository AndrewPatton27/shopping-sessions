#!/usr/bin/env python3
"""Metamorphic dataset transforms; pipeline output must be identical to the source's.
Usage: meta.py SRC_DATA OUT_DATA --relation {shuffle,split,headers,requote,reencode,all} [--seed N]
Byte-exact: files read in binary, split on newline only, CR stays data, headers kept."""
import argparse
import random
import re
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

KINDS = ("events", "clicks", "links")
DEFAULT_HEADER = {"events": b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
                  "clicks": b"click_id,user_id,campaign_id,ts",
                  "links": b"user_id_a,user_id_b"}
NFIELDS = {"events": 7, "clicks": 4}
TS_COL = {"events": 2, "clicks": 3}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
D = rb"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
F1 = re.compile(D + rb"Z")
F2 = re.compile(D + rb"([+-])([0-9]{2}):([0-9]{2})")
F3 = re.compile(rb"[0-9]+")


def load(src):
    """data[kind] = {'header': bytes, 'files': [list of data lines]}; zero-byte files are dropped."""
    data = {}
    for kind in KINDS:
        header, files = None, []
        for p in sorted((src / kind).glob("*.csv")):
            raw = p.read_bytes()
            if not raw:
                continue
            if raw.endswith(b"\n"):
                raw = raw[:-1]
            lines = raw.split(b"\n")
            header = header or lines[0]
            files.append(lines[1:])
        data[kind] = {"header": header or DEFAULT_HEADER[kind], "files": files}
    return data


def write(data, src, out):
    out.mkdir(parents=True)
    for item in src.iterdir():
        if item.name not in KINDS:
            (shutil.copytree if item.is_dir() else shutil.copy2)(item, out / item.name)
    for kind in KINDS:
        d = out / kind
        d.mkdir()
        for i, lines in enumerate(data[kind]["files"]):
            with open(d / f"part-{i:05d}.csv", "wb") as f:
                f.write(data[kind]["header"] + b"\n")
                for ln in lines:
                    f.write(ln + b"\n")


def cut(lines, k, rng):
    pts = sorted(rng.randint(0, len(lines)) for _ in range(k - 1))
    return [lines[a:b] for a, b in zip([0] + pts, pts + [len(lines)])]


def shuffle(data, rng):
    for kind in KINDS:
        files = data[kind]["files"]
        allv = [ln for f in files for ln in f]
        rng.shuffle(allv)
        data[kind]["files"] = cut(allv, max(len(files), 1), rng)


def split(data, rng):
    for kind in KINDS:
        files = data[kind]["files"]
        allv = [ln for f in files for ln in f]
        data[kind]["files"] = cut(allv, rng.randint(1, max(2 * len(files), 2)), rng)


def headers(data, rng):
    for kind in KINDS:
        for _ in range(rng.randint(1, 3)):
            data[kind]["files"].insert(rng.randint(0, len(data[kind]["files"])), [])


def whole_quoted(f):
    return len(f) >= 2 and f[0] == 34 and f[-1] == 34 and b'"' not in f[1:-1]


def plain_line(fields):
    """True when every quote in the line sits around a whole field."""
    return all(b'"' not in f or whole_quoted(f) for f in fields)


def requote(data, rng, p=0.3):
    def one(line):
        fields = line.split(b",")
        if not line or not plain_line(fields):
            return line
        return b",".join(b'"' + f + b'"' if b'"' not in f and rng.random() < p else f for f in fields)
    for kind in KINDS:
        data[kind]["files"] = [[one(ln) for ln in f] for f in data[kind]["files"]]


def parse_ts(v):
    """valid whole-second timestamp -> UTC datetime, else None."""
    try:
        m = F1.fullmatch(v)
        if m:
            y, mo, d, h, mi, s = map(int, m.groups())
            return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)
        m = F2.fullmatch(v)
        if m:
            y, mo, d, h, mi, s = map(int, m.groups()[:6])
            oh, om = int(m.group(8)), int(m.group(9))
            if oh > 23 or om > 59:
                return None
            off = timedelta(hours=oh, minutes=om) * (1 if m.group(7) == b"+" else -1)
            return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc) - off
        if F3.fullmatch(v):
            ms = int(v)
            if ms % 1000 or ms > 253402300799000:
                return None
            return EPOCH + timedelta(seconds=ms // 1000)
    except (ValueError, OverflowError):
        return None
    return None


def render(dt, rng):
    form = rng.choice((1, 2, 3))
    try:
        if form == 1:
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ").encode()
        if form == 2:
            mins = rng.randint(-(23 * 60 + 59), 23 * 60 + 59)
            sign = "-" if mins < 0 or (mins == 0 and rng.random() < 0.5) else "+"
            loc = dt + timedelta(minutes=mins)
            a = abs(mins)
            return f"{loc.strftime('%Y-%m-%dT%H:%M:%S')}{sign}{a // 60:02d}:{a % 60:02d}".encode()
    except (ValueError, OverflowError):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ").encode()
    s = str(int((dt - EPOCH).total_seconds()) * 1000)
    if rng.random() < 0.3:
        s = s.zfill(len(s) + rng.randint(1, 5))
    return s.encode()


def reencode(data, rng):
    for kind in TS_COL:
        col, n = TS_COL[kind], NFIELDS[kind]

        def one(line):
            fields = line.split(b",")
            if len(fields) != n or not plain_line(fields):
                return line
            f = fields[col]
            quoted = whole_quoted(f)
            dt = parse_ts(f[1:-1] if quoted else f)
            if dt is None:
                return line
            new = render(dt, rng)
            fields[col] = b'"' + new + b'"' if quoted else new
            return b",".join(fields)
        data[kind]["files"] = [[one(ln) for ln in f] for f in data[kind]["files"]]


REL = {"shuffle": shuffle, "split": split, "headers": headers, "requote": requote, "reencode": reencode}
ALL_ORDER = ("requote", "reencode", "shuffle", "split", "headers")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("src")
    ap.add_argument("out")
    ap.add_argument("--relation", required=True, choices=[*REL, "all"])
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    src, out = Path(a.src), Path(a.out)
    if out.exists():
        sys.exit(f"meta.py: {out} exists")
    rng = random.Random(a.seed)
    data = load(src)
    for name in (ALL_ORDER if a.relation == "all" else (a.relation,)):
        REL[name](data, rng)
    write(data, src, out)
    print(f"meta {a.relation} seed={a.seed}: " +
          ", ".join(f"{k}={len(data[k]['files'])} files" for k in KINDS))


if __name__ == "__main__":
    main()
