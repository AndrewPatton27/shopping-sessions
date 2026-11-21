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

if "NCPU" in os.environ:
    os.environ.setdefault("POLARS_MAX_THREADS", os.environ["NCPU"])

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

# Polars treats CRLF as a line end, but rules.md says CR is an ordinary
# character. Per file, swap it for a byte proven absent from that file and swap it
# back right after parsing.
_FREE = [bytes([c]) for c in range(0x1F, 0, -1) if c not in (0x0A, 0x0D, 0x09)]


def _absent(body, taken=()):
    for c in _FREE:
        if c not in taken and body.find(c) < 0:
            return c
    raise RuntimeError("no free marker byte")


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
    sep = _absent(body)
    mark = None
    if b"\x0d" in body:
        mark = _absent(body, (sep,))
        body = body.replace(b"\x0d", mark)
    df = pl.read_csv(
        io.BytesIO(body), has_header=False, separator=sep.decode(), quote_char=None,
        new_columns=["line"], schema={"line": pl.String}, eol_char="\n",
    )
    if mark is not None:
        df = df.with_columns(pl.col("line").str.replace_all(mark.decode(), "\x0d", literal=True))
    return df


def parse_lines(lines, names):
    """Raw one-column line frame -> DataFrame of nullable strings; malformed lines dropped."""
    n = len(names)
    # A line passing the shape check has exactly n-1 commas and quotes only around whole
    # fields, so dropping every quote leaves the field values; "" and empty are both null.
    cols = [pl.when(pl.col(f"field_{i}") == "").then(None).otherwise(pl.col(f"field_{i}")).alias(name)
            for i, name in enumerate(names)]
    return (lines.lazy().filter(pl.col("line").str.contains(_line_re(n)))
            .select(pl.col("line").str.replace_all('"', "", literal=True).str.split_exact(",", n - 1).alias("s"))
            .unnest("s").select(cols).collect())


def _read_parsed(path, names):
    lines = _read_file_lines(path)
    return None if lines is None else parse_lines(lines, names)


def read_table(paths, names):
    """Parse CSV files into a DataFrame of nullable strings; malformed lines dropped."""
    paths = [p for p in paths]
    with ThreadPoolExecutor(min(len(paths), NCPU) or 1) as ex:
        frames = [f for f in ex.map(lambda p: _read_parsed(p, names), paths) if f is not None]
    if not frames:
        return pl.DataFrame(schema={n: pl.String for n in names})
    return pl.concat(frames, rechunk=False)


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


class _Collision(Exception):
    pass


EV_NAMES = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
EV_SCHEMA = {"h1": pl.UInt64, "h2": pl.UInt64, "ing": pl.Int64, "uh": pl.UInt64, "t": pl.Int64,
             "purchase": pl.Boolean, "price": pl.Int64, "cat": pl.Int32, "ok": pl.Boolean}


def _event_file(path, seed, w_start, w_end, prod_old, prod_new, unk):
    """One events file -> (numeric frame, unique user_id strings).

    Rows carry hashes of event_id (two 64-bit, for dedup) and user_id (resolved to a
    person later), parsed times, and `ok`: whether this copy would pass validation.
    Validation is applied only after dedup, as the rules order it.
    """
    lines = _read_file_lines(path)
    if lines is None:
        return None
    df = parse_lines(lines, EV_NAMES)
    del lines
    users = df["user_id"].drop_nulls().unique()
    s = pl.col
    lf = (df.lazy().filter(s("event_id").is_not_null())
          .pipe(add_ts, "ingest_ts", "ing").pipe(add_ts, "ts", "t")
          .with_columns(price=pl.when(s("price_cents").str.contains(r"^[0-9]+$"))
                        .then(s("price_cents").cast(pl.Int64, strict=False)),
                        purchase=s("event_type") == "purchase"))
    ok = (s("user_id").is_not_null() & s("product_id").is_not_null()
          & s("event_type").is_in(["view", "cart", "purchase"])
          & s("t").is_not_null() & (s("t") >= w_start) & (s("t") < w_end)
          & (~s("purchase") | (s("price").is_not_null() & (s("price") >= 1) & (s("price") <= 10_000_000)))
          & ((s("ing") - s("t")) <= DAY_MS))
    out = lf.select(
        s("event_id").hash(seed).alias("h1"), s("event_id").hash(seed + 1).alias("h2"), "ing",
        s("user_id").hash(seed + 2).alias("uh"), "t", "purchase",
        pl.when(s("purchase")).then(s("price")).otherwise(0).fill_null(0).alias("price"),
        s("product_id").replace_strict(prod_old, prod_new, default=unk, return_dtype=pl.Int32).alias("cat"),
        ok.alias("ok"),
    ).collect()
    return out, users


def main():
    for seed in range(0, 1000, 7):
        try:
            return run(seed)
        except _Collision:
            log("hash collision, retrying with another seed")
    raise RuntimeError("no collision-free hash seed")


def run(seed):
    with open(os.path.join(DATA, "manifest.json")) as f:
        man = json.load(f)
    w_start = parse_iso_scalar(man["window_start"])
    w_end = parse_iso_scalar(man["window_end"])
    c_start = w_start - WEEK_MS
    log("window", w_start, w_end)

    products = read_table([os.path.join(DATA, "products.csv")], ["product_id", "category"])
    campaigns = read_table([os.path.join(DATA, "campaigns.csv")], ["campaign_id", "channel"])

    # Category codes follow byte order, so the smaller code is the byte-wise smaller name.
    products = products.drop_nulls("product_id").with_columns(pl.col("category").fill_null("unknown"))
    catnames = sorted(set(products["category"].to_list()) | {"unknown"})
    unk = catnames.index("unknown")
    cat_code = {c: i for i, c in enumerate(catnames)}
    prod_old = products["product_id"]
    prod_new = pl.Series([cat_code[c] for c in products["category"].to_list()], dtype=pl.Int32)

    # ---------------------------------------------------------------- events (per file)
    paths = files("events")
    with ThreadPoolExecutor(min(len(paths), NCPU) or 1) as ex:
        res = [r for r in ex.map(lambda p: _event_file(p, seed, w_start, w_end, prod_old, prod_new, unk), paths)
               if r is not None]
    N = pl.concat([r[0] for r in res], rechunk=False) if res else pl.DataFrame(schema=EV_SCHEMA)
    ev_users = pl.concat([r[1] for r in res]) if res else pl.Series("user_id", [], dtype=pl.String)
    del res
    log("events parsed", N.height)

    # Dedup on event_id: keep the copy with the smallest ingest_ts, then validate it.
    # Only rows whose first hash repeats can have copies; the exact (h1, h2) dedup runs on those.
    N = N.filter(pl.col("ing").is_not_null())
    dup = N["h1"].is_duplicated()
    keep = ["uh", "t", "purchase", "price", "cat"]
    single = N.filter(~dup & pl.col("ok")).select(keep)
    multi = (N.filter(dup).sort("ing").unique(["h1", "h2"], keep="first", maintain_order=True)
              .filter(pl.col("ok")).select(keep))
    N = pl.concat([single, multi], rechunk=False)
    del single, multi
    log("deduped", N.height)

    links = read_table(files("links"), ["a", "b"]).drop_nulls()
    ck = read_table(files("clicks"), ["click_id", "user_id", "campaign_id", "ts"])

    # ------------------------------------------------------------ linked IDs
    # Every ID that appears anywhere is a node; person = connected component.
    U = pl.concat([s.alias("u") for s in (ev_users, ck["user_id"].drop_nulls(), links["a"], links["b"])]).unique()
    uh = U.hash(seed + 2)
    if uh.n_unique() != U.len():
        raise _Collision
    n_ids = U.len()
    if links.height:
        UI = pl.DataFrame({"uh": uh, "i": pl.arange(0, n_ids, eager=True, dtype=pl.Int64)})
        lk = (links.select(ha=pl.col("a").hash(seed + 2), hb=pl.col("b").hash(seed + 2))
              .join(UI.rename({"uh": "ha", "i": "ia"}), on="ha")
              .join(UI.rename({"uh": "hb", "i": "ib"}), on="hb"))
        _sp_thread.join()
        g = _sp["coo"]((np.ones(lk.height, dtype=np.int32), (lk["ia"].to_numpy(), lk["ib"].to_numpy())),
                       shape=(n_ids, n_ids))
        _, comp = _sp["cc"](g, directed=True, connection="weak")
        comp = comp.astype(np.int64)
    else:
        comp = np.arange(n_ids, dtype=np.int64)
    UC = pl.DataFrame({"uh": uh, "pi": pl.Series(comp), "uid": U})
    # Person name: byte-wise smallest of their IDs.
    names = UC.group_by("pi").agg(name=pl.col("uid").min())
    UC = UC.select("uh", "pi")
    n_people = int(comp.max()) + 1 if n_ids else 0

    # ---------------------------------------------------------------- sessions
    N = N.join(UC, on="uh", how="left").drop("uh")
    SH2 = 1 << max(1, int(w_end - w_start).bit_length())
    assert n_people * SH2 < (1 << 63), "packed sort key would overflow"
    N = (N.with_columns(k=pl.col("pi") * SH2 + (pl.col("t") - w_start)).sort("k"))
    n = N.height
    c = N["pi"].to_numpy()
    t = N["t"].to_numpy()
    price = N["price"].to_numpy()
    purchase = N["purchase"].to_numpy()
    cat = N["cat"].to_numpy()
    del N
    if n:
        new = np.empty(n, dtype=bool)
        new[0] = True
        new[1:] = (c[1:] != c[:-1]) | ((t[1:] - t[:-1]) > GAP_MS)
        starts = np.flatnonzero(new)
        sid = np.cumsum(new) - 1
        s_events = np.diff(np.append(starts, n))
        s_rev = np.add.reduceat(price, starts)
        s_purch = np.add.reduceat(purchase.astype(np.int64), starts) > 0
        s_pi = c[starts]
        s_start = t[starts]
    else:
        sid = np.zeros(0, dtype=np.int64)
        s_events = s_rev = s_pi = s_start = np.zeros(0, dtype=np.int64)
        s_purch = np.zeros(0, dtype=bool)
    s_dayi = s_start // DAY_MS
    day_of = lambda a: pl.Series(a.astype(np.int32)).cast(pl.Date)
    log("sessions", len(s_pi))

    # ---------------------------------------------------------------- clicks
    # Campaigns by integer index (-1 is organic); a click on an unknown campaign is dropped by the join.
    campaigns = campaigns.drop_nulls("campaign_id")
    camp_idx = campaigns.select("campaign_id").with_row_index("cidx").with_columns(pl.col("cidx").cast(pl.Int64))
    ck = (ck.lazy()
          .filter(pl.col("click_id").is_not_null())
          .unique("click_id", keep="any")
          .pipe(add_ts, "ts", "t")
          .filter(pl.col("user_id").is_not_null() & pl.col("t").is_not_null()
                  & (pl.col("t") >= c_start) & (pl.col("t") < w_end))
          .join(camp_idx.lazy(), on="campaign_id", how="inner")
          .select("click_id", pl.col("user_id").hash(seed + 2).alias("uh"), "cidx", "t")
          .collect()
          .join(UC, on="uh", how="left").drop("uh"))

    # One sortable i64 key per (person, time).
    base = c_start - DAY_MS
    # Largest offset in a key: assist upper bound = last click (< w_end) + 7 days.
    SHIFT = 1 << (int(w_end + WEEK_MS - base) + 1).bit_length()
    assert n_people * SHIFT < (1 << 63), "packed sort key would overflow"

    def key(col):
        return pl.col("pi") * SHIFT + (pl.col(col) - base)

    sessions = pl.DataFrame({"pi": s_pi, "start": s_start, "purchased": s_purch, "revenue": s_rev,
                             "events": s_events}).with_columns(k=key("start"))
    ck = ck.with_columns(k=key("t"))

    # Credit: latest click at or before start within 7 days; tie -> smallest click_id.
    credit_clicks = (ck.sort("k", "click_id").unique("k", keep="first", maintain_order=True)
                       .select("k", cpi="pi", ct="t", cidx="cidx"))
    cred = sessions.join_asof(credit_clicks, on="k", strategy="backward")
    cred = cred.with_columns(
        day=day_of(s_dayi),
        cidx=pl.when((pl.col("cpi") == pl.col("pi")) & (pl.col("start") - pl.col("ct") <= WEEK_MS))
        .then(pl.col("cidx")).otherwise(-1))

    # Assists: merge each (person, campaign)'s [ts, ts+7d] intervals, map to session index ranges.
    sk = sessions["k"]
    iv = (ck.lazy().select("pi", "cidx", "t").unique().sort("pi", "cidx", "t")
            .with_columns(brk=((pl.col("pi") != pl.col("pi").shift(1)) | (pl.col("cidx") != pl.col("cidx").shift(1))
                               | ((pl.col("t") - pl.col("t").shift(1)) > WEEK_MS)).fill_null(True))
            .with_columns(grp=pl.col("brk").cum_sum())
            .group_by("grp").agg(pl.col("pi").first(), pl.col("cidx").first(),
                                 a=pl.col("t").min(), b=pl.col("t").max() + WEEK_MS)
            .collect())
    lo = sk.search_sorted(iv["pi"] * SHIFT + (iv["a"] - base), side="left").to_numpy().astype(np.int64)
    hi = sk.search_sorted(iv["pi"] * SHIFT + (iv["b"] - base), side="right").to_numpy().astype(np.int64)
    cnt = np.maximum(hi - lo, 0)
    total = int(cnt.sum())
    n_camp = camp_idx.height
    day0 = w_start // DAY_MS
    n_days = (w_end - 1) // DAY_MS - day0 + 1
    if total:
        cum = np.cumsum(cnt) - cnt
        rows = np.repeat(lo - cum, cnt) + np.arange(total, dtype=np.int64)
        cid = np.repeat(iv["cidx"].to_numpy(), cnt)
        # Sessions start inside the window, so (campaign, day) fits a small dense grid.
        bc = np.bincount(cid * n_days + (s_dayi[rows] - day0), minlength=n_camp * n_days)
        nz = np.flatnonzero(bc)
        assist = pl.DataFrame({"cidx": nz // n_days, "day": day_of(day0 + nz % n_days),
                               "assisted_sessions": bc[nz]})
    else:
        assist = pl.DataFrame(schema={"cidx": pl.Int64, "day": pl.Date, "assisted_sessions": pl.Int64})

    credited = (cred.group_by("cidx", "day").agg(
                    sessions=pl.len(), purchasing_sessions=pl.col("purchased").sum().cast(pl.Int64),
                    revenue_cents=pl.col("revenue").sum()))
    camp_names = pl.concat([campaigns.select("campaign_id", "channel").with_row_index("cidx")
                            .with_columns(pl.col("cidx").cast(pl.Int64)),
                            pl.DataFrame({"cidx": [-1], "campaign_id": ["organic"], "channel": ["organic"]},
                                         schema={"cidx": pl.Int64, "campaign_id": pl.String, "channel": pl.String})])
    camp_daily = (credited.join(assist, on=["cidx", "day"], how="full", coalesce=True)
                  .with_columns(pl.col("sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions")
                                .fill_null(0).cast(pl.Int64))
                  .join(camp_names, on="cidx", how="left")
                  .with_columns(channel=pl.col("channel").fill_null("organic"))
                  .select("campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                          "revenue_cents", "assisted_sessions"))

    # ------------------------------------------------------------ user_daily
    pidx = np.flatnonzero(purchase)
    top = (pl.DataFrame({"pi": c[pidx], "day": day_of(s_dayi[sid[pidx]]), "cat": cat[pidx], "price": price[pidx]})
           .group_by("pi", "day", "cat").agg(c=pl.col("price").sum())
           .group_by("pi", "day").agg(
               top_cat=pl.col("cat").sort_by(["c", "cat"], descending=[True, False]).first())
           )
    top = top.with_columns(top_category=pl.Series(catnames, dtype=pl.String).gather(top["top_cat"])).drop("top_cat")
    user_daily = (pl.DataFrame({"pi": s_pi, "day": day_of(s_dayi), "revenue": s_rev, "events": s_events})
                  .group_by("pi", "day").agg(
                      sessions=pl.len().cast(pl.Int64), events=pl.col("events").sum().cast(pl.Int64),
                      revenue_cents=pl.col("revenue").sum().cast(pl.Int64))
                  .join(top, on=["pi", "day"], how="left")
                  .join(names, on="pi", how="left")
                  .rename({"name": "user_id"})
                  .select("user_id", "day", "sessions", "events", "revenue_cents", "top_category"))

    os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
    user_daily.write_parquet(os.path.join(OUT, "user_daily", "part-0.parquet"))
    camp_daily.write_parquet(os.path.join(OUT, "campaign_daily", "part-0.parquet"))
    log("wrote", user_daily.height, camp_daily.height)


if __name__ == "__main__":
    main()
