# Handoff (overwrite each session; keep under 30 lines)

Updated: 2026-11-21 by Opus lead.

## Done
- Repo github.com/AndrewPatton27/shopping-sessions (PRIVATE; user must OK public before any submission). Latest: 7617bfc.
- Native Rust (std only) is the shipped engine: Dockerfile builds it (x86-64-v3). Polars fallback: Dockerfile.polars.
- Rust: full verify + META PASS; official 1M dev 0.1 s; 10M 0.9 s (Polars 7.8 s), outputs identical.
- Test kit: oracle sha256 guard, fixtures 36, regress R1-R4, official dev holdout, invariants, metamorphic, FAST mode, bench lock.

## In progress
- Opus reviewer: read-only audit of native/ (unsafe blocks, rules, Parquet validity, 60M hazards).
- Sonnet: perf round on native/src (owns native/ only).

## Next 3 steps
1. Apply reviewer findings (to Sonnet as owner), re-verify, commit.
2. Time 20M official set; keep peak RSS well under 24 GB at 60M.
3. When user makes repo public: submit (max 1/hour), log score in context/SUBMISSIONS.tsv.

## Do not retry
- Fable model: billed to paid extra usage ($1.82 spent, cap hit). Banned (D-010).
- Capture-group regex in Polars; CR_MARK global swap (breaks byte order); fixed SHIFT=2^31.
- Absolute PIPE paths with spaces in verify.sh: use `./native/...`.

## Resume
`cd starter/starter-duckdb && PIPE=./native/target-v3/release/shopping-sessions.exe bash scripts/verify.sh`
Usage rule: 5-hour >= 75% cheap tier only; >= 95% stop all agents until reset.
