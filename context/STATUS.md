# Status

Updated: 2026-11-21 by Opus lead. Event closes Sun 2026-11-22 11:00 AM EST.

## Entry
- Track: Databricks Shopping Sessions. Score = container wall time (~20M practice, ~60M final, 3-min cap).
- Repo: github.com/AndrewPatton27/shopping-sessions (PRIVATE until user OKs public; submissions need public).
- Shipped engine: native Rust (std only), `Dockerfile` (x86-64-v3). Fallback: Polars, `Dockerfile.polars`.

## Evidence
| Check | Rust | Polars |
|---|---|---|
| fixtures 36 / regress 4 | pass | pass |
| official 1M dev answer | MATCH, 0.1 s | MATCH, ~2 s |
| seeds 1-3 vs reference | MATCH | MATCH |
| metamorphic x6 | MATCH | MATCH |
| fuzz | 400 seeds pass | 200 seeds pass |
| official-gen 10M | ~0.45-0.8 s | ~7.8 s |
| official-gen 60M | 8.4 s, 8.9 GB peak | 85 s; output MATCH Rust |
| GitHub CI linux/amd64, scorer flags | build + fixtures + regress pass | same |

## Open items
- Repo public + submit (user decision; max 1/hour).
- Further Rust speedups (parse stage is the largest).
