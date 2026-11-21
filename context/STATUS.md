# Status

Event: Builders Buildathon (joinbuilders.org). Submissions close Sun 2026-11-22 11:00 AM EST.

## Track

Chosen: Databricks - Shopping Sessions.

Turn messy click-stream CSVs into two exact daily Parquet tables: `user_daily` and `campaign_daily`.

- Score: container run time on about 20M events (final about 60M).
- Hard cap: 3 min per run.
- Baseline: 172 s.
- Submissions: 2 per hour.
- Hand-in: public GitHub repo with a Dockerfile at the root.

Tracks seen: Builders Mech Royale, Databricks Shopping Sessions, Machina CAD De-ID, RWX Blast Radius.

## Why this track

- Exact spec with one right answer.
- 36 provided edge-case fixtures.
- A published dev answer, so correctness can be tested locally without guessing.
- Other tracks need 3D game AI, 139.7 GB of CAD data, or heuristic test selection.

## Layout

- `downloads/` - `rules.md`, original zips.
- `starter/starter-duckdb/` - the entry repo.
  - `pipeline/pipeline.py` - Polars pipeline.
  - `pipeline/reference.py` - pure-Python literal oracle.
  - `scripts/run_fixtures.py`, `scripts/compare.py`, `scripts/stress_gen.py` (our random dirty-data generator).
  - `fixtures/`
- `gen/generator/` - official Rust generator source. Not built.
- `ans/dev-answer/` - official expected output for the 1M dev set.
- `stress/` - generated test datasets and outputs. Not for the repo.

## How to test

Run in `starter/starter-duckdb`:

1. Fixtures: `python scripts/run_fixtures.py fixtures -- python pipeline/pipeline.py`
2. Generate data: `python scripts/stress_gen.py ../../stress/dN --seed N --events 200000`
3. Run `reference.py` and `pipeline.py` with `DATA_DIR` and `OUTPUT_DIR` env vars.
4. Compare: `cd scripts && python compare.py REF_OUT POL_OUT`

## Results so far

- `reference.py`: 36/36 fixtures.
- `pipeline.py`: 36/36 fixtures.
- Reference vs pipeline: MATCH on 3 random 200k-event seeds and on a 2M-event set (108,325 `user_daily` rows, 854 `campaign_daily` rows).
- Self-check sums equal in both tables: sessions 248,644; revenue 17,609,388,496.
- Polars timing on 2M local: 13.2 s -> 7.7 s after replacing the capture-group timestamp regex.
- Reference takes 43.6 s on 2M.

## Open items

- Official dev set not generated. Building and running the downloaded Rust generator was blocked by the auto-mode permission classifier. User can allow it.
- Docker not installed locally, so the image was never built.
- No GitHub repo created and no submission made. Needs user.
- Performance work in progress (Sonnet agent on `pipeline.py`).
- Quirk: Polars `read_csv` strips trailing `\r`. The pipeline swaps `\r` for `\x1e` before parsing and swaps back in outputs.
