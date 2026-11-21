# Decisions (append-only)

## D-001 2026-11-21 Track: Databricks Shopping Sessions
Why: exact spec, 36 fixtures and a dev answer make correctness locally checkable. Rejected: Mech Royale (3D game AI), Machina (139.7 GB data), RWX (heuristic, scored on hidden failures).
Status: accepted

## D-002 2026-11-21 Pure-Python oracle before the fast pipeline
Why: rules are exact and a near-flawless verifier is needed for differential tests. Evidence: reference.py 36/36 fixtures; 43.6 s on 2M.
Status: accepted

## D-003 2026-11-21 Engine: Polars + scipy, not DuckDB or PySpark
Why: vectorized string parsing for the strict CSV dialect, join_asof for credit, scipy connected_components for linked IDs. Evidence: 36/36; MATCH vs reference on seeds 1-3 (200k) and 2M.
Status: accepted (Rust/C++ rewrite is the next step up if Polars plateaus)

## D-004 2026-11-21 Assists via merged click intervals + search_sorted
Why: a sessions x campaigns cross join explodes for whales and shared devices; merged [ts, ts+7d] intervals per (person, campaign) expand to exactly the assisted sessions.
Status: accepted

## D-005 2026-11-21 Own stress generator instead of the official one
Why: building/running the downloaded Rust generator was blocked by the auto-mode classifier. stress_gen.py targets the rules.md edge cases. Gap: no check against the official dev answer yet.
Status: accepted until the user allows the generator

## D-006 2026-11-21 Repo = starter/starter-duckdb, private until submit
Why: Dockerfile must sit at repo root; context/ synced in via scripts/sync_context.sh. Private until the user okays going public (submission requires public).
Status: accepted

## D-007 2026-11-21 Parallel native Rust track (Fable) in native/
Why: leaderboard tops ~0.65-1 s on 20M; Polars ~7.7 s on 2M locally. Rust toolchain already installed. Isolated dir, so no conflict with the single pipeline.py writer.
Status: in progress

## D-008 2026-11-21 Keep full Rust track despite research-engine-options "stop at <120 s"
Why: the goal is rank, not passing; leaderboard tops are ~0.65-1 s on 20M. Fallback if full Rust stalls: hybrid d1 (Rust parse -> typed columns, Polars logic).
Status: accepted

## D-009 2026-11-21 No paid credits: plan usage only
Why: user order. Extra usage is enabled on the account ($18.49 of $20 spent), and it only kicks in after the 5-hour limit fills.
Rule: lead checks get_usage at every checkpoint. 5-hour >= 75%: no new Opus/Fable agents, cheap tier or Codex only (per codex-fallback memory). >= 88%: stop all agents, wait for the reset.
Status: accepted
