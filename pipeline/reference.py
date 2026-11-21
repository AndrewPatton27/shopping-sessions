#!/usr/bin/env python3
"""
Shopping Sessions: pure-Python reference implementation of rules.md.

Slow but literal. Used as the correctness oracle for the fast pipeline.
Reads $DATA_DIR, writes $OUTPUT_DIR/user_daily and $OUTPUT_DIR/campaign_daily.
"""
import bisect
import glob
import json
import os
import re
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS

# ---------------------------------------------------------------- CSV dialect


def read_rows(path, ncols):
    """Yield parsed rows (list of str|None) of one CSV file; malformed lines dropped."""
    with open(path, "rb") as f:
        raw = f.read().decode("utf-8")
    lines = raw.split("\n")
    if lines and lines[-1] == "":
        lines.pop()  # trailing newline terminates the last line, it is not a line
    for line in lines[1:]:  # first line is the header
        row = parse_line(line, ncols)
        if row is not None:
            yield row


def parse_line(line, ncols):
    if line == "":
        return None
    fields = line.split(",")
    if len(fields) != ncols:
        return None
    out = []
    for f in fields:
        if '"' in f:
            if len(f) < 2 or f[0] != '"' or f[-1] != '"' or '"' in f[1:-1]:
                return None
            f = f[1:-1]
        out.append(f if f != "" else None)
    return out


def read_dir(sub, ncols):
    for path in sorted(glob.glob(os.path.join(DATA, sub, "*.csv"))):
        yield from read_rows(path, ncols)


# ----------------------------------------------------------------- timestamps

_ISO = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|[+-][0-9]{2}:[0-9]{2})")
_DIGITS = re.compile(r"[0-9]+")


def days_from_civil(y, m, d):
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def is_leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


MDAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def parse_ts(s):
    """Return epoch ms, or None if the value is a bad timestamp form."""
    if s is None:
        return None
    if _DIGITS.fullmatch(s):
        return int(s)
    m = _ISO.fullmatch(s)
    if not m:
        return None
    y, mo, d, h, mi, se = (int(m.group(i)) for i in range(1, 7))
    if not (1 <= mo <= 12) or h > 23 or mi > 59 or se > 59:
        return None
    dim = 29 if (mo == 2 and is_leap(y)) else MDAYS[mo - 1]
    if not (1 <= d <= dim):
        return None
    ms = ((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000
    z = m.group(7)
    if z != "Z":
        oh, om = int(z[1:3]), int(z[4:6])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        ms -= off if z[0] == "+" else -off
    return ms


def ms_to_date(ms):
    return ms // DAY_MS  # days since epoch (date32)


# ---------------------------------------------------------------------- main


def main():
    with open(os.path.join(DATA, "manifest.json")) as f:
        man = json.load(f)
    w_start = parse_ts(man["window_start"])
    w_end = parse_ts(man["window_end"])

    products = {r[0]: r[1] for r in read_rows(os.path.join(DATA, "products.csv"), 2)}
    campaigns = {r[0]: r[1] for r in read_rows(os.path.join(DATA, "campaigns.csv"), 2)}

    # Linked IDs: union-find, person named by the smallest ID.
    parent = {}

    def find(x):
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while parent.get(x, x) != root:
            parent[x], x = root, parent[x]
        return root

    for a, b in read_dir("links", 2):
        if a is None or b is None:
            continue
        parent.setdefault(a, a)
        parent.setdefault(b, b)
        ra, rb = find(a), find(b)
        if ra != rb:
            # keep the smaller ID as root, so the root is the person's name
            if rb < ra:
                ra, rb = rb, ra
            parent[rb] = ra

    def person(uid):
        return find(uid) if uid in parent else uid

    # Events: parse, dedup on smallest ingest_ts, validate, drop late.
    best = {}
    for row in read_dir("events", 7):
        eid = row[0]
        if eid is None:
            continue
        ing = parse_ts(row[3])
        cur = best.get(eid)
        if cur is None or ing < cur[0]:
            best[eid] = (ing, row)

    per_person = defaultdict(list)  # person -> [(ts, is_purchase, price, category)]
    for ing, (eid, uid, ts_s, _, etype, pid, price_s) in best.values():
        if uid is None or pid is None or etype not in ("view", "cart", "purchase"):
            continue
        ts = parse_ts(ts_s)
        if ts is None or not (w_start <= ts < w_end):
            continue
        price = 0
        if etype == "purchase":
            if price_s is None or not _DIGITS.fullmatch(price_s):
                continue
            price = int(price_s)
            if not (1 <= price <= 10_000_000):
                continue
        if ing - ts > DAY_MS:
            continue
        per_person[person(uid)].append((ts, etype == "purchase", price, products.get(pid, "unknown")))

    # Clicks: parse, dedup, validate.
    seen_clicks = {}
    for cid, uid, camp, ts_s in read_dir("clicks", 4):
        if cid is None or cid in seen_clicks:
            continue
        seen_clicks[cid] = (uid, camp, ts_s)
    clicks_by_person = defaultdict(list)
    for cid, (uid, camp, ts_s) in seen_clicks.items():
        if uid is None or camp not in campaigns:
            continue
        ts = parse_ts(ts_s)
        if ts is None or not (w_start - WEEK_MS <= ts < w_end):
            continue
        clicks_by_person[person(uid)].append((ts, cid, camp))
    for v in clicks_by_person.values():
        v.sort()

    user_daily = {}  # (person, day) -> [sessions, events, revenue, {cat: cents}]
    camp_daily = {}  # (campaign, day) -> [sessions, purchasing, revenue, assisted]

    def camp_row(c, day):
        k = (c, day)
        r = camp_daily.get(k)
        if r is None:
            r = camp_daily[k] = [0, 0, 0, 0]
        return r

    for p, evs in per_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > GAP_MS:
                sessions.append([])
            sessions[-1].append(e)
            prev = e[0]
        clicks = clicks_by_person.get(p, [])
        cts = [c[0] for c in clicks]
        for s in sessions:
            start = s[0][0]
            day = ms_to_date(start)
            rev = sum(e[2] for e in s if e[1])
            purchased = any(e[1] for e in s)
            ud = user_daily.get((p, day))
            if ud is None:
                ud = user_daily[(p, day)] = [0, 0, 0, defaultdict(int)]
            ud[0] += 1
            ud[1] += len(s)
            ud[2] += rev
            for e in s:
                if e[1]:
                    ud[3][e[3]] += e[2]
            lo = bisect.bisect_left(cts, start - WEEK_MS)
            hi = bisect.bisect_right(cts, start)
            window = clicks[lo:hi]
            if window:
                top_ts = window[-1][0]
                credit = min(c for c in window if c[0] == top_ts)[2]
                for camp in {c[2] for c in window}:
                    camp_row(camp, day)[3] += 1
            else:
                credit = "organic"
            r = camp_row(credit, day)
            r[0] += 1
            r[1] += purchased
            r[2] += rev

    def top_cat(cats):
        if not cats:
            return None
        return min(cats.items(), key=lambda kv: (-kv[1], kv[0]))[0]

    os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
    uk = list(user_daily.items())
    pq.write_table(pa.table({
        "user_id": pa.array([k[0] for k, _ in uk], pa.string()),
        "day": pa.array([k[1] for k, _ in uk], pa.date32()),
        "sessions": pa.array([v[0] for _, v in uk], pa.int64()),
        "events": pa.array([v[1] for _, v in uk], pa.int64()),
        "revenue_cents": pa.array([v[2] for _, v in uk], pa.int64()),
        "top_category": pa.array([top_cat(v[3]) for _, v in uk], pa.string()),
    }), os.path.join(OUT, "user_daily", "part-0.parquet"))
    ck = list(camp_daily.items())
    pq.write_table(pa.table({
        "campaign_id": pa.array([k[0] for k, _ in ck], pa.string()),
        "channel": pa.array([campaigns.get(k[0], "organic") for k, _ in ck], pa.string()),
        "day": pa.array([k[1] for k, _ in ck], pa.date32()),
        "sessions": pa.array([v[0] for _, v in ck], pa.int64()),
        "purchasing_sessions": pa.array([v[1] for _, v in ck], pa.int64()),
        "revenue_cents": pa.array([v[2] for _, v in ck], pa.int64()),
        "assisted_sessions": pa.array([v[3] for _, v in ck], pa.int64()),
    }), os.path.join(OUT, "campaign_daily", "part-0.parquet"))


if __name__ == "__main__":
    main()
