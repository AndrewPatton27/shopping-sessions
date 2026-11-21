# Handoff (overwrite each session; keep under 30 lines)

Updated: 2026-11-21 by Opus lead.

## Done
- Track chosen; reference.py + pipeline.py both 36/36; MATCH on seeds 1-3 and 2M set.
- Dockerfile/requirements moved to Polars (not built: no Docker locally).

## In progress
- Sonnet: speed work on `starter/starter-duckdb/pipeline/pipeline.py` (owns only that file).
- Haiku: `scripts/verify.sh` + `.claude/skills/verify-pipeline/SKILL.md`.

## Next 3 steps
1. Run `bash scripts/verify.sh` on Sonnet's result; fresh read-only reviewer vs rules.md.
2. Add `scripts/bench.sh` + `bench/results.tsv` (median of 3, append-only).
3. User: allow the official generator, install Docker or create the GitHub repo, then submit.

## Do not retry
- Capture-group regex for timestamps (6 s of 13 s on 2M).
- winget rustup install (killed, exit 0xC000013A); rustup-init -y works.
- Building/running gen/generator without user permission (classifier block).

## Resume
`cd starter/starter-duckdb && bash scripts/verify.sh`. Nothing committed; no git repo for the entry yet.
