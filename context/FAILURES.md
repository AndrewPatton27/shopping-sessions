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
