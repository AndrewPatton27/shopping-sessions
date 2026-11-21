#!/usr/bin/env python3
"""
Random dirty dataset for differential testing (pipeline.py vs reference.py).

Not the official generator: it aims at the edge cases in rules.md (dup events,
bad timestamps, all three ts forms, quotes, CR, malformed lines, link chains,
boundary clicks, equal timestamps) rather than realistic distributions.

Usage: stress_gen.py OUT_DIR [--seed N] [--events N]
"""
import argparse
import json
import os
import random

DAY = 86_400_000
WEEK = 7 * DAY


def iso(ms, rnd):
    """Render epoch ms as F1, F2 or F3 (F1/F2 only when ms has whole seconds)."""
    import datetime as dt
    if ms % 1000 or rnd.random() < 0.3:
        s = str(ms)
        return ("000" + s) if rnd.random() < 0.05 else s
    t = dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc)
    if rnd.random() < 0.7:
        return t.strftime("%Y-%m-%dT%H:%M:%SZ")
    off = rnd.randint(-10 * 60, 12 * 60) // 15 * 15
    lt = t + dt.timedelta(minutes=off)
    sign = "+" if off >= 0 else "-"
    a = abs(off)
    return lt.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{a // 60:02d}:{a % 60:02d}"


BAD_TS = ["", "2026-10-03 10:00:00", "2026-10-03T10:00:00z", "2026-10-03T10:00:00.5Z", "2026-13-01T00:00:00Z",
          "2026-02-29T00:00:00Z", "2026-10-03T24:00:00Z", "2026-10-03T10:00:60Z", "2026-10-03T10:00:00+24:00",
          "2026-10-03T10:00:00+05:60", "2026-10-03T10:00:00+0200", "1.79e12", "-5", "1791071890000.0",
          "86400000", "123456789012345678901234", "2026-10-03T10:00:00"]


def dirty(v, rnd):
    if v != "" and rnd.random() < 0.15:
        return f'"{v}"'
    if v == "" and rnd.random() < 0.1:
        return '""'
    return v


def line(fields, rnd):
    if rnd.random() < 0.003:
        k = rnd.randint(0, 3)
        if k == 0:
            return ""
        if k == 1:
            return ",".join(fields[:-1])
        if k == 2:
            return ",".join(fields) + ',x'
        return ",".join(fields[:1] + ['"' + fields[1]] + fields[2:])
    out = ",".join(dirty(f, rnd) for f in fields)
    if rnd.random() < 0.002:
        out += "\r"
    return out


def write_parts(dirname, header, rows, rnd, nfiles):
    os.makedirs(dirname, exist_ok=True)
    rnd.shuffle(rows)
    parts = [[] for _ in range(nfiles)]
    for r in rows:
        parts[rnd.randrange(nfiles)].append(r)
    for i, p in enumerate(parts):
        with open(os.path.join(dirname, f"part-{i:05d}.csv"), "w", encoding="utf-8", newline="") as f:
            f.write(header + "\n")
            for r in p:
                f.write(r + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--events", type=int, default=200_000)
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    w_start = 1_791_072_000_000 - rnd.randint(0, 400) * DAY
    w_start -= w_start % DAY
    w_end = w_start + 14 * DAY
    out = a.out
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "manifest.json"), "w") as f:
        import datetime as dt
        fmt = lambda ms: dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        json.dump({"window_start": fmt(w_start), "window_end": fmt(w_end)}, f)

    cats = [f"cat{i}" for i in range(8)] + ["Books", "unknownish"]
    prods = [f"p{i}" for i in range(500)]
    with open(os.path.join(out, "products.csv"), "w", newline="") as f:
        f.write("product_id,category\n")
        for p in prods:
            f.write(f"{p},{rnd.choice(cats)}\n")
    camps = [f"c_{i}" for i in range(60)]
    with open(os.path.join(out, "campaigns.csv"), "w", newline="") as f:
        f.write("campaign_id,channel\n")
        for c in camps:
            f.write(f"{c},{rnd.choice(['search', 'social', 'email', 'display', 'affiliate'])}\n")

    npeople = max(10, a.events // 30)
    people = []
    links = []
    for pi in range(npeople):
        k = rnd.choice([1] * 45 + [2] * 30 + [3] * 13 + [4, 5, 6, 7, 8] * 2)
        if rnd.random() < 0.001:
            k = rnd.randint(50, 200)
        ids = [f"u{pi}_{j}" for j in range(k)]
        if rnd.random() < 0.01:
            ids.append(f"U{pi}_0")  # upper-case variant: a different ID
        for j in range(1, len(ids)):
            x, y = ids[rnd.randrange(j)], ids[j]
            links.append(f"{x},{y}" if rnd.random() < 0.5 else f"{y},{x}")
            if rnd.random() < 0.08:
                links.append(f"{y},{x}")
        if rnd.random() < 0.01:
            links.append(f"{ids[0]},{ids[0]}")
        if rnd.random() < 0.05:
            links.append(f"{ids[0]},ghost{pi}")
        people.append(ids)
    # shared devices
    for d in range(3):
        dev = f"dev{d}"
        for pi in rnd.sample(range(npeople), max(1, npeople // 250)):
            links.append(f"{rnd.choice(people[pi])},{dev}")
    links += [",u1_0", "u2_0,", "a,b,c"]
    link_rows = [line(l.split(","), rnd) if l.count(",") == 1 else l for l in links]
    write_parts(os.path.join(out, "links"), "user_id_a,user_id_b", link_rows, rnd, 3)

    ev_rows = []
    ck_rows = []
    eid = 0
    cid = 0
    while eid < a.events:
        pi = int(rnd.paretovariate(1.2)) % npeople if rnd.random() < 0.5 else rnd.randrange(npeople)
        ids = people[pi]
        t = rnd.randint(w_start - DAY, w_end + DAY)
        if rnd.random() < 0.3:
            t -= t % 1000
        nsess = rnd.randint(1, 4)
        for _ in range(nsess):
            # boundary / regular clicks before session start
            for _ in range(rnd.randint(0, 3)):
                off = rnd.choice([WEEK, WEEK + 1, 0, -1, rnd.randint(0, WEEK), rnd.randint(0, 3600_000)])
                ct = t - off
                cid += 1
                camp = rnd.choice(camps) if rnd.random() > 0.01 else rnd.choice(["c_999", "C_1", ""])
                cts = iso(ct, rnd) if rnd.random() > 0.005 else rnd.choice(BAD_TS)
                uid = rnd.choice(ids) if rnd.random() > 0.002 else ""
                row = [f"k{cid}", uid, camp, cts]
                ck_rows.append(line(row, rnd))
                if rnd.random() < 0.02:
                    ck_rows.append(",".join(f'"{x}"' if x else x for x in row))
                if rnd.random() < 0.02:  # same-ms twin click
                    cid += 1
                    ck_rows.append(line([f"k{cid}", rnd.choice(ids), rnd.choice(camps), str(ct)], rnd))
            for _ in range(rnd.randint(1, 12)):
                g = rnd.choice([0, 0, rnd.randint(1, 600_000), 1_800_000, 1_800_001, rnd.randint(1000, 900_000)])
                t += g
                eid += 1
                etype = rnd.choice(["view"] * 6 + ["cart"] * 2 + ["purchase"])
                if rnd.random() < 0.002:
                    etype = rnd.choice(["Purchase", "cart ", "click"])
                price = str(rnd.randint(100, 50_000)) if etype == "purchase" else rnd.choice(["", "", "-5", "1.5"])
                if etype == "purchase" and rnd.random() < 0.05:
                    price = rnd.choice(["", "0", "19.99", "-100", "10000001", "0250", "10000000"])
                prod = rnd.choice(prods) if rnd.random() > 0.005 else rnd.choice(["p9999", "P1", ""])
                ing = t + rnd.randint(0, 3600_000)
                if rnd.random() < 0.01:
                    ing = t + rnd.choice([DAY, DAY + 1000, rnd.randint(DAY, 5 * DAY)])
                ing -= ing % 1000
                ts_s = iso(t, rnd) if rnd.random() > 0.005 else rnd.choice(BAD_TS)
                uid = rnd.choice(ids) if rnd.random() > 0.002 else ""
                row = [f"e{eid}", uid, ts_s, iso(ing, random.Random(0)) if False else fmt_f1(ing), etype, prod, price]
                if rnd.random() < 0.03:
                    for _ in range(rnd.randint(1, 3)):
                        dup = list(row)
                        dup[3] = fmt_f1(ing + rnd.randint(1, 600) * 1000)
                        if rnd.random() < 0.05:
                            dup[1] = rnd.choice(people[rnd.randrange(npeople)])
                        if rnd.random() < 0.3:
                            dup[2] = iso(t, rnd)
                        ev_rows.append(line(dup, rnd))
                    if rnd.random() < 0.1:
                        row[2] = rnd.choice(BAD_TS)
                ev_rows.append(line(row, rnd))
            t += rnd.randint(1_800_001, 2 * DAY)
    write_parts(os.path.join(out, "events"), "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
                ev_rows, rnd, 7)
    write_parts(os.path.join(out, "clicks"), "click_id,user_id,campaign_id,ts", ck_rows, rnd, 4)
    # header-only file
    with open(os.path.join(out, "events", "part-99999.csv"), "w", newline="") as f:
        f.write("event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n")


def fmt_f1(ms):
    import datetime as dt
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


if __name__ == "__main__":
    main()
