# Changelog

- 2026-11-21: Picked Databricks Shopping Sessions track; downloaded rules.md, starter-duckdb, dev-answer, generator.
- 2026-11-21: Installed Rust (rustup, GNU toolchain) to build the generator; build/run then blocked by permission classifier.
- 2026-11-21: Wrote `pipeline/reference.py` (pure-Python oracle); 36/36 fixtures.
- 2026-11-21: Wrote `pipeline/pipeline.py` (Polars + scipy connected_components for linked IDs, join_asof for credit, merged click intervals + search_sorted for assists); 36/36 fixtures.
- 2026-11-21: Wrote `scripts/stress_gen.py`; reference vs Polars MATCH on seeds 1-3 (200k) and 2M set.
- 2026-11-21: Sped timestamp parsing (capture-free regex + fixed slices): 13.2 s -> 7.7 s on 2M.
- 2026-11-21: Split remaining work: Sonnet perf on `pipeline.py`, Haiku Dockerfile/requirements, Haiku context docs; Opus final review.
- 2026-11-21: Haiku moved Dockerfile/requirements/README/TEAM.md to Polars (polars 2.0.0, numpy 2.2.6, scipy 1.15.3, pyarrow 25.0.1).
- 2026-11-21: Applied context/youtube best practices: added compact project CLAUDE.md; Haiku building scripts/verify.sh + .claude/skills/verify-pipeline skill; watching context/ for new notes.
- 2026-11-21: Read agent-team-research.md + notes-A: plan = fresh read-only reviewer (correctness vs rules.md, no style nits) after Sonnet perf pass; lead waits for workers; short path+summary reports. Hook-gated verify deferred (too slow mid-edit).
