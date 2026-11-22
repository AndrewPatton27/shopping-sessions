# Handoff (overwrite each session; keep under 30 lines)

Updated: 2026-11-22 ~01:30Z by Opus lead. Deadline Sun 2026-11-22 16:00Z (11:00 AM EST).

## Done
- Best scored: 0.84 s (commit 3da92db / 2ec16fe, entered in final), rank ~76-81/334. Leaders 0.30 s.
- main (0831e2d) native/ == 2ec16fe (best). Repo PUBLIC, no attribution lines.
- Submissions log: context/SUBMISSIONS.tsv. Rule: max 1/hour, only when faster.

## Merged wins (scorer): exp-direct, exp-fresh (eid-partitioned parse), exp-bc (1-line link probes, NT scatter), exp-mem/mem2/mem3 (memory traffic/footprint).
## Rejected on scorer: exp-br (branch removal), exp-rec2 (24 B records), exp-simd (AVX-512 batch parse; CI -9%, scorer slower).
## Rejected on CI: mmap, fadvise, SWWC plain, write-once, ymm AVX-512, AVX2 batch, safe buffer tweaks.

## Key facts
- Final retimes chosen commit 3x on 60M new events; practice rank is not the prize.
- CI (EPYC) mispredicts scorer (Xeon) gains: memory-traffic cuts pay more on scorer; SIMD batch did not.
- Verify: verify.sh (local), verify-linux workflow (CI, AVX-512), bench.yml same-run A/B, docker.yml.
- Usage rule: no paid credits (Fable banned); 5h >= 75% cheap tier, >= 95% stop.

## Next (if continuing)
1. Only structural ideas remain; three agents estimate ~3x needs a different parse design.
2. Before deadline: confirm best commit is the one entered in the final (it is by default).
