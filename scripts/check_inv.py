#!/usr/bin/env python3
"""Invariants on a pipeline output (no oracle). Usage: check_inv.py OUT_DIR [MANIFEST_JSON]
OUT_DIR holds user_daily/ and campaign_daily/ parquet folders, or user_daily.csv / campaign_daily.csv.
Prints 'INV PASS' or 'INV FAIL <reason>'; exit 0 / 1."""
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

USER = {"user_id": pl.String, "day": pl.Date, "sessions": None, "events": None,
        "revenue_cents": None, "top_category": pl.String}
CAMP = {"campaign_id": pl.String, "channel": pl.String, "day": pl.Date, "sessions": None,
        "purchasing_sessions": None, "revenue_cents": None, "assisted_sessions": None}


class Fail(Exception):
    pass


def load(folder, table):
    csv = folder / f"{table}.csv"
    if csv.is_file():
        df = pl.read_csv(csv, schema_overrides={"day": pl.String})
        return df.with_columns(pl.col("day").str.to_date())
    files = sorted(p for p in (folder / table).glob("*.parquet")
                   if p.is_file() and not p.name.startswith((".", "_")))
    if not files:
        raise Fail(f"{table}: no parquet files")
    return pl.concat([pl.read_parquet(p) for p in files], how="vertical_relaxed")


def need(cond, msg):
    if not cond:
        raise Fail(msg)


def check(out, manifest):
    u, c = load(out, "user_daily"), load(out, "campaign_daily")
    for name, df, spec in (("user_daily", u, USER), ("campaign_daily", c, CAMP)):
        need(set(df.columns) == set(spec) and len(df.columns) == len(spec),
             f"{name}: columns {sorted(df.columns)}")
        need(df.schema["day"] == pl.Date, f"{name}: day is {df.schema['day']}, not Date")
        for col, typ in spec.items():
            if typ is None:
                need(df.schema[col].is_integer(), f"{name}.{col}: not integer ({df.schema[col]})")
            else:
                need(df.schema[col] == typ, f"{name}.{col}: type {df.schema[col]}")
        for col in df.columns:
            if df.schema[col].is_integer():
                need(df[col].null_count() == 0, f"{name}.{col}: null")
                need(df.height == 0 or df[col].min() >= 0, f"{name}.{col}: negative")
    need(u.n_unique(subset=["user_id", "day"]) == u.height, "user_daily: duplicate (user_id, day)")
    need(c.n_unique(subset=["campaign_id", "day"]) == c.height, "campaign_daily: duplicate (campaign_id, day)")
    need(u["user_id"].null_count() == 0 and (u["user_id"] == "").sum() == 0, "user_daily: empty user_id")
    need(c["campaign_id"].null_count() == 0 and c["channel"].null_count() == 0,
         "campaign_daily: null campaign/channel")
    if u.height:
        need(u["sessions"].min() >= 1, "user_daily: sessions < 1")
        need((u["events"] >= u["sessions"]).all(), "user_daily: events < sessions")
        need((u["top_category"].is_null() == (u["revenue_cents"] == 0)).all(),
             "user_daily: top_category null != (revenue == 0)")
    ts_u, ts_c = int(u["sessions"].sum() or 0), int(c["sessions"].sum() or 0)
    need(ts_u == ts_c, f"sum sessions differ: user {ts_u} vs campaign {ts_c}")
    tr_u, tr_c = int(u["revenue_cents"].sum() or 0), int(c["revenue_cents"].sum() or 0)
    need(tr_u == tr_c, f"sum revenue differ: user {tr_u} vs campaign {tr_c}")
    if c.height:
        nonorg = c.filter(pl.col("campaign_id") != "organic")
        need(int(c["assisted_sessions"].sum()) >= int(nonorg["sessions"].sum()),
             "sum assisted_sessions < credited non-organic sessions")
        need((c["purchasing_sessions"] <= c["sessions"]).all(), "campaign_daily: purchasing_sessions > sessions")
        org = c.filter(pl.col("campaign_id") == "organic")
        need(org.height == 0 or int(org["assisted_sessions"].sum()) == 0, "organic has assisted_sessions")
        need((org["channel"] == "organic").all(), "organic channel != organic")
        need(((c["sessions"] > 0) | (c["assisted_sessions"] > 0)).all(),
             "campaign_daily: row with no sessions and no assists")
        need(((c["sessions"] > 0) | ((c["purchasing_sessions"] == 0) & (c["revenue_cents"] == 0))).all(),
             "campaign_daily: revenue without credited sessions")
    if manifest:
        m = json.loads(Path(manifest).read_text())

        def parse(s):
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        lo = parse(m["window_start"]).date()
        hi = (parse(m["window_end"]) - timedelta(milliseconds=1)).date()
        for name, df in (("user_daily", u), ("campaign_daily", c)):
            if df.height:
                need(df["day"].min() >= lo and df["day"].max() <= hi,
                     f"{name}: day outside window {lo}..{hi} ({df['day'].min()}..{df['day'].max()})")


def main():
    if len(sys.argv) not in (2, 3):
        print("usage: check_inv.py OUT_DIR [MANIFEST_JSON]", file=sys.stderr)
        return 2
    try:
        check(Path(sys.argv[1]), sys.argv[2] if len(sys.argv) == 3 else None)
    except Fail as e:
        print(f"INV FAIL {e}")
        return 1
    except Exception as e:  # unreadable output is a failure too
        print(f"INV FAIL {type(e).__name__}: {e}")
        return 1
    print("INV PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
