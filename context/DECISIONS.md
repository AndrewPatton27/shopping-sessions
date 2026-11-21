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
