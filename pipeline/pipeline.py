#!/usr/bin/env python3
"""
Shopping Sessions: Polars pipeline implementing rules.md.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet

Checked for equality against pipeline/reference.py (a literal pure-Python version).
"""
import glob
import io
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import threading

import numpy as np
import polars as pl

_sp = {}


def _import_scipy():
    # scipy.sparse takes ~1.5 s to import; do it while Polars reads CSV (GIL released).
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components
    _sp["coo"], _sp["cc"] = coo_matrix, connected_components


_sp_thread = threading.Thread(target=_import_scipy)
_sp_thread.start()

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS

# Polars treats "\r\n" as a line end, but rules.md says "\r" is an ordinary
# character. Swap it for a byte the generator never writes, and swap it back
# in output strings.
CR_MARK = "\x1e"


def log(*a):
    print(*a, file=sys.stderr, flush=True)


# ---------------------------------------------------------------- CSV dialect


def _line_re(n):
    # No captures (they are slow): n fields, each bare or wrapped whole in quotes.
    f = r'(?:"[^",]*"|[^",]*)'
    return "^" + ",".join([f] * n) + "$"


def _read_file_lines(path):
    with open(path, "rb") as fh:
        raw = fh.read()
    nl = raw.find(b"\n")
    if nl < 0:
        return None  # header only, no newline
    body = raw[nl + 1:]
    if not body:
        return None
    if b"\r" in body:
        body = body.replace(b"\r", CR_MARK.encode())
    return pl.read_csv(
        io.BytesIO(body), has_header=False, separator="\x1f", quote_char=None,
        new_columns=["line"], schema={"line": pl.String}, eol_char="\n",
    )


def read_table(paths, names):
    """Parse CSV files into a DataFrame of nullable strings; malformed lines dropped."""
    paths = [p for p in paths]
    with ThreadPoolExecutor(min(len(paths), NCPU) or 1) as ex:
        frames = [f for f in ex.map(_read_file_lines, paths) if f is not None]
    if not frames:
        return pl.DataFrame(schema={n: pl.String for n in names})
    lines = pl.concat(frames, rechunk=False)
    n = len(names)
    # A line passing the shape check has exactly n-1 commas and quotes only around whole
    # fields, so dropping every quote leaves the field values; "" and empty are both null.
    cols = [pl.when(pl.col(f"field_{i}") == "").then(None).otherwise(pl.col(f"field_{i}")).alias(name)
            for i, name in enumerate(names)]
    return (lines.lazy().filter(pl.col("line").str.contains(_line_re(n)))
            .select(pl.col("line").str.replace_all('"', "", literal=True).str.split_exact(",", n - 1).alias("s"))
            .unnest("s").select(cols).collect())


def files(sub):
    return sorted(glob.glob(os.path.join(DATA, sub, "*.csv")))


# ----------------------------------------------------------------- timestamps

_TS_RE = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})$"


def add_ts(lf, col, out):
    """Add Int64 column `out`: epoch ms of string column `col`, null for a bad form.

    Staged with_columns (shared intermediates) and the native datetime parser for
    the calendar check; the regex shape check fixes every offset and keeps the
    parser strict, the explicit second/offset checks cover what it accepts.
    """
    R, N = f"_r{out}", f"_n{out}"
    lf = lf.with_columns(
        pl.col(col).alias(R),
        pl.col(col).str.contains(r"^[0-9]+$").alias(f"_d{out}"),
        pl.col(col).str.contains(_TS_RE).alias(f"_s{out}"),
        pl.col(col).str.len_bytes().alias(N),
    ).with_columns(
        pl.when(pl.col(f"_d{out}")).then(pl.col(R).cast(pl.Int64, strict=False)).alias(f"_g{out}"),
        pl.when(pl.col(f"_s{out}")).then(pl.col(R)).alias(f"_i{out}"),
    )
    i = pl.col(f"_i{out}")
    lf = lf.with_columns(
        i.str.slice(0, 19).str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=False, time_unit="ms")
         .dt.epoch("ms").alias(f"_b{out}"),
        i.str.slice(17, 2).cast(pl.Int64, strict=False).alias(f"_se{out}"),
        i.str.slice(20, 2).cast(pl.Int64, strict=False).alias(f"_oh{out}"),
        i.str.slice(23, 2).cast(pl.Int64, strict=False).alias(f"_om{out}"),
        i.str.slice(19, 1).alias(f"_sg{out}"),
    )
    base, se, oh, om, sg = (pl.col(f"_{k}{out}") for k in ("b", "se", "oh", "om", "sg"))
    n = pl.col(N)
    off = (oh * 60 + om) * 60_000
    ok = (se <= 59) & ((n == 20) | ((n == 25) & (oh <= 23) & (om <= 59)))
    iso = pl.when(base.is_not_null() & ok).then(
        pl.when(sg == "+").then(base - off).when(sg == "-").then(base + off).otherwise(base))
    lf = lf.with_columns(pl.coalesce(pl.col(f"_g{out}"), iso).alias(out))
    return lf.drop([R, N] + [f"_{k}{out}" for k in ("d", "s", "g", "i", "b", "se", "oh", "om", "sg")])


def parse_iso_scalar(v):
    return pl.DataFrame({"x": [v]}, schema={"x": pl.String}).lazy().pipe(add_ts, "x", "t").collect()["t"].item()


# ---------------------------------------------------------------------- main


def main():
    with open(os.path.join(DATA, "manifest.json")) as f:
        man = json.load(f)
    w_start = parse_iso_scalar(man["window_start"])
    w_end = parse_iso_scalar(man["window_end"])
    c_start = w_start - WEEK_MS
    log("window", w_start, w_end)

    products = read_table([os.path.join(DATA, "products.csv")], ["product_id", "category"])
    campaigns = read_table([os.path.join(DATA, "campaigns.csv")], ["campaign_id", "channel"])

    ev = read_table(files("events"), ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"])

    # ------------------------------------------------------------ linked IDs
    links = read_table(files("links"), ["a", "b"]).drop_nulls()
    if links.height:
        ids = pl.concat([links["a"], links["b"]]).unique().sort()
        id_df = pl.DataFrame({"uid": ids, "idx": pl.arange(0, len(ids), eager=True, dtype=pl.Int64)})
        lk = (links.join(id_df.rename({"uid": "a", "idx": "ia"}), on="a")
                   .join(id_df.rename({"uid": "b", "idx": "ib"}), on="b"))
        n = len(ids)
        _sp_thread.join()
        g = _sp["coo"]((np.ones(lk.height, dtype=np.int32), (lk["ia"].to_numpy(), lk["ib"].to_numpy())), shape=(n, n))
        _, comp = _sp["cc"](g, directed=True, connection="weak")
        # ids is sorted, so the first idx of each component is its smallest ID
        id_df = id_df.with_columns(comp=pl.Series(comp.astype(np.int64)))
        name_of = id_df.with_columns(name=pl.col("uid").first().over("comp")).select("uid", "name")
    else:
        name_of = pl.DataFrame(schema={"uid": pl.String, "name": pl.String})

    def with_person(lf):
        return (lf.join(name_of.lazy(), left_on="user_id", right_on="uid", how="left")
                  .with_columns(person=pl.coalesce("name", "user_id")).drop("name"))

    # ---------------------------------------------------------------- events
    ev = (ev.lazy()
          .filter(pl.col("event_id").is_not_null())
          .pipe(add_ts, "ingest_ts", "ing")
          .sort("ing", nulls_last=True)
          .unique("event_id", keep="first", maintain_order=True)
          .filter(pl.col("ing").is_not_null())
          .pipe(add_ts, "ts", "t")
          .with_columns(price=pl.when(pl.col("price_cents").str.contains(r"^[0-9]+$"))
                        .then(pl.col("price_cents").cast(pl.Int64, strict=False)))
          .filter(pl.col("user_id").is_not_null() & pl.col("product_id").is_not_null()
                  & pl.col("event_type").is_in(["view", "cart", "purchase"])
                  & pl.col("t").is_not_null() & (pl.col("t") >= w_start) & (pl.col("t") < w_end)
                  & ((pl.col("event_type") != "purchase")
                     | (pl.col("price").is_not_null() & (pl.col("price") >= 1) & (pl.col("price") <= 10_000_000)))
                  & ((pl.col("ing") - pl.col("t")) <= DAY_MS))
          .with_columns(purchase=pl.col("event_type") == "purchase")
          .with_columns(price=pl.when(pl.col("purchase")).then(pl.col("price")).otherwise(0))
          .select("user_id", "t", "purchase", "price", "product_id"))
    ev = with_person(ev).sort("person", "t")
    ev = (ev.with_columns(
            new=(pl.col("person") != pl.col("person").shift(1)).fill_null(True)
                | ((pl.col("t") - pl.col("t").shift(1)) > GAP_MS))
          .with_columns(sid=pl.col("new").cum_sum())
          .join(products.lazy(), on="product_id", how="left")
          .with_columns(category=pl.col("category").fill_null("unknown"))
          .collect())
    log("events", ev.height)

    sessions = (ev.lazy().group_by("sid").agg(
                    person=pl.col("person").first(), start=pl.col("t").min(), events=pl.len(),
                    revenue=pl.col("price").sum(), purchased=pl.col("purchase").any())
                .with_columns(day=(pl.col("start").floordiv(DAY_MS)).cast(pl.Int32).cast(pl.Date))
                .sort("person", "start")
                .collect())
    log("sessions", sessions.height)

    # ---------------------------------------------------------------- clicks
    ck = read_table(files("clicks"), ["click_id", "user_id", "campaign_id", "ts"])
    ck = (ck.lazy()
          .filter(pl.col("click_id").is_not_null())
          .unique("click_id", keep="any")
          .pipe(add_ts, "ts", "t")
          .filter(pl.col("user_id").is_not_null() & pl.col("t").is_not_null()
                  & (pl.col("t") >= c_start) & (pl.col("t") < w_end))
          .join(campaigns.lazy().select("campaign_id"), on="campaign_id", how="semi")
          .select("click_id", "user_id", "campaign_id", "t"))
    ck = with_person(ck).collect()

    # Dense person index shared by sessions and clicks, for one sortable i64 key.
    persons = pl.concat([sessions["person"], ck["person"]]).unique().sort()
    pidx = pl.DataFrame({"person": persons, "pi": pl.arange(0, len(persons), eager=True, dtype=pl.Int64)})
    SHIFT = 1 << 31
    base = c_start - DAY_MS

    def key(t):
        return pl.col("pi") * SHIFT + (pl.col(t) - base)

    sessions = sessions.join(pidx, on="person").with_columns(k=key("start")).sort("k")
    ck = ck.join(pidx, on="person").with_columns(k=key("t"))

    # Credit: latest click at or before start within 7 days; tie -> smallest click_id.
    credit_clicks = (ck.sort("k", "click_id").unique("k", keep="first", maintain_order=True)
                       .select("k", cpi="pi", ct="t", campaign_id="campaign_id"))
    cred = sessions.join_asof(credit_clicks.select("k", "cpi", "ct", "campaign_id"), on="k", strategy="backward")
    cred = cred.with_columns(
        campaign_id=pl.when((pl.col("cpi") == pl.col("pi")) & (pl.col("start") - pl.col("ct") <= WEEK_MS))
        .then(pl.col("campaign_id")).otherwise(pl.lit("organic")))

    # Assists: merge each (person, campaign)'s [ts, ts+7d] intervals, map to session index ranges.
    sk = sessions["k"]
    iv = (ck.lazy().select("pi", "campaign_id", "t").unique().sort("pi", "campaign_id", "t")
            .with_columns(brk=((pl.col("pi") != pl.col("pi").shift(1)) | (pl.col("campaign_id") != pl.col("campaign_id").shift(1))
                               | ((pl.col("t") - pl.col("t").shift(1)) > WEEK_MS)).fill_null(True))
            .with_columns(grp=pl.col("brk").cum_sum())
            .group_by("grp").agg(pl.col("pi").first(), pl.col("campaign_id").first(),
                                 a=pl.col("t").min(), b=pl.col("t").max() + WEEK_MS)
            .collect())
    lo = sk.search_sorted(iv["pi"] * SHIFT + (iv["a"] - base), side="left")
    hi = sk.search_sorted(iv["pi"] * SHIFT + (iv["b"] - base), side="right")
    sdays = sessions["day"]
    assist = (iv.select("campaign_id").with_columns(lo=lo.cast(pl.Int64), hi=hi.cast(pl.Int64))
                .filter(pl.col("hi") > pl.col("lo"))
                .with_columns(row=pl.int_ranges("lo", "hi")).explode("row")
                .with_columns(day=pl.col("row").map_batches(lambda r: sdays.gather(r), return_dtype=pl.Date))
                .group_by("campaign_id", "day").agg(assisted_sessions=pl.len()))

    credited = (cred.group_by("campaign_id", "day").agg(
                    sessions=pl.len(), purchasing_sessions=pl.col("purchased").sum().cast(pl.Int64),
                    revenue_cents=pl.col("revenue").sum()))
    camp_daily = (credited.join(assist, on=["campaign_id", "day"], how="full", coalesce=True)
                  .with_columns(pl.col("sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions")
                                .fill_null(0).cast(pl.Int64))
                  .join(campaigns, on="campaign_id", how="left")
                  .with_columns(channel=pl.col("channel").fill_null("organic"))
                  .select("campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                          "revenue_cents", "assisted_sessions"))

    # ------------------------------------------------------------ user_daily
    sday = sessions.select("sid", "day")
    top = (ev.lazy().filter(pl.col("purchase")).join(sday.lazy(), on="sid")
             .group_by("person", "day", "category").agg(c=pl.col("price").sum())
             .group_by("person", "day").agg(
                 top_category=pl.col("category").sort_by(["c", "category"], descending=[True, False]).first())
             .collect())
    user_daily = (sessions.group_by("person", "day").agg(
                      sessions=pl.len().cast(pl.Int64), events=pl.col("events").sum().cast(pl.Int64),
                      revenue_cents=pl.col("revenue").sum().cast(pl.Int64))
                  .join(top, on=["person", "day"], how="left")
                  .rename({"person": "user_id"})
                  .select("user_id", "day", "sessions", "events", "revenue_cents", "top_category"))

    def unmark(df, cols):
        return df.with_columns([pl.col(c).str.replace_all(CR_MARK, "\r", literal=True) for c in cols])

    user_daily = unmark(user_daily, ["user_id", "top_category"])
    camp_daily = unmark(camp_daily, ["campaign_id", "channel"])

    os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
    user_daily.write_parquet(os.path.join(OUT, "user_daily", "part-0.parquet"))
    camp_daily.write_parquet(os.path.join(OUT, "campaign_daily", "part-0.parquet"))
    log("wrote", user_daily.height, camp_daily.height)


if __name__ == "__main__":
    main()
