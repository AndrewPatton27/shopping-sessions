# Regression cases

Cases made from bugs found by two code reviews of `pipeline.py`. Same layout and CSV format as `fixtures/`. Expected tables come from `pipeline/reference.py` (the oracle), not by hand. Run with `python scripts/run_fixtures.py regress -- python pipeline/reference.py`.

| Case | Window | Catches | Source |
| --- | --- | --- | --- |
| R1 | 2026-10-01 to 2026-10-31 (30 days) | Sort key overflow past about 24.8 days: one person's click or session is counted for another person. Expected: `c_1` and `c_2` each credited and assisted once | `stress/audit-rules/t4.py`, review finding 1 |
| R2 | 2026-10-01 to 2026-10-15 | Link names `"x\r"` (0x0D) and `x\x10` (0x10): the byte-wise smallest name (`x\r`) must name the person | Review T2 (`scratchpad/review/T2`), rules ordering |
| R3 | 2026-10-01 to 2026-10-15 | Literal 0x1E byte in a user ID round-trips unchanged (not turned into `\r`) | `context/review-rules-audit.md` finding 2 |
| R4 | 2026-10-01 to 2026-10-15 | Literal 0x1F byte in an event line: no crash, the line is data and counts for `u\x1f2` | `context/review-rules-audit.md` finding 3 |

Note: review T1 (30-day window, credit and assist overflow) uses the same bug class as R1. R1 is the minimal reproducer from `t4.py`; T1 was not made into a separate case.

Against the audited copy `stress/audit-rules/pipeline_v2.py`, R1 to R4 all fail. Against the current `pipeline/pipeline.py`, all four pass.
