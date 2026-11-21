# Failures (append-only; cluster and fix once)

Format: date | who | what failed | root cause | fix / rule

## Tooling / environment
- 2026-11-21 | lead | winget rustup install exit 0xC000013A | installer killed in non-interactive shell | use rustup-init.exe -y (GNU host)
- 2026-11-21 | lead | build/run of downloaded generator blocked | auto-mode classifier treats downloaded code as untrusted | ask user (granted later); never work around
- 2026-11-21 | lead | context watcher reported "C:/Users/andre/code/Ohio" and false repeats | path with space split by awk; comm needs whole-line sort | path-first tab output + LC_ALL=C sort
- 2026-11-21 | lead | near-miss: global core.autocrlf would rewrite fixtures (deliberate \r) and run.sh | Windows git default | repo core.autocrlf=false + .gitattributes `* -text`; check bytes after staging

## Data / parsing
- 2026-11-21 | lead | Polars read_csv strips trailing \r | engine treats \r\n as EOL | swap \r -> \x1e before parse, back in outputs (fixture E20 guards)

## Agent process
- 2026-11-21 | haiku | verify.sh hid pipeline stderr on crash | brief said /dev/null | briefs must say where failure detail goes (log tail on FAIL)
- 2026-11-21 | other session audit | packed key SHIFT=2^31 overflows for windows >17.9 d; \x1e/\x1f bytes break CR_MARK/separator | fixed-width bit packing and sentinel bytes assumed, not derived | derive widths from data; choose sentinels proven absent (assigned to Sonnet round 2)
- 2026-11-21 | lead | ~$1.82 of paid extra usage spent (18.78 -> 20.60, cap hit); Fable agent died on 'individual spend limit' 429 | Fable usage likely billed to extra usage, not plan; lead only checked usage at events, none fired for a long stretch | never use Fable model here; Sonnet/Haiku/Opus only; check usage on every agent event
- 2026-11-21 | lead | regress/*.csv never committed (gitignore *.csv) -> fresh clone lacked test inputs; caught by new GitHub CI | ignore rule written before regress/ existed | CI on clean checkout is the guard; unignore regress/**/*.csv
- 2026-11-21 | sonnet | Rust parse round: F1 date cache (<=1-4%, unresolvable), NCPU 12/16 oversubscription (worse), reader threads (no gain) -> none kept | laptop noise 425-840 ms on identical runs | local timing has plateaued; need quiet machine or scorer feedback
- 2026-11-21 | sonnet | exp-mmap (Linux mmap+MAP_POPULATE, MADV_HUGEPAGE, 4MB BufWriter): same-run CI A/B 1.06-1.13 s vs main 1.07 s -> not faster, rejected (branch kept) | page-cache copy was not the bottleneck | A/B only within one CI run; runners differ 0.67 vs 1.07 s for same code
- 2026-11-21 | opus | incremental tuning plateaued (rounds 2-4 <=3% each) | anchored on existing architecture | use a fresh-context agent for redesign (best-practices: fresh mind on a stuck problem)
- 2026-11-21 | opus-fresh | exp-fresh2 write-once (person lookup + 2 writes in parse loop): CI +4-7% slower | in-loop lookups/writes cost more than separate pass | keep parse lookup-free; leaders' edge likely raw parse speed
- 2026-11-21 | sonnet | exp-swwc software write-combining scatter: +7% slower (1420 vs 1321) | extra copy + count branch > miss savings at this partition count | don't retry SWWC; scatter not the lever
- 2026-11-21 | lead | switched shared checkout to apply a peer patch while an Opus agent worked in it; its commit landed on the wrong branch, patch half-applied | two writers in one working tree | test outside work in a separate git worktree (git worktree add ../wt-<name>); never switch branches in the shared checkout while an agent runs
- 2026-11-21 | opus | exp-simd AVX-512 batch parse: CI -9% (EPYC Zen4), scorer +2% slower | Zen4 AVX-512 has no downclock; Xeon may | for scorer, prefer AVX2/scalar wins; if retrying AVX-512, test AVX2-only build of same design first
