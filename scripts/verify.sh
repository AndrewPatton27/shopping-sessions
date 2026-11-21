#!/usr/bin/env bash
# Verify the Shopping Sessions pipeline: oracle guard, fixtures, official dev holdout, invariants,
# then differential test vs reference.py on seeds 1-3.
# Run from starter/starter-duckdb: bash scripts/verify.sh
#   FAST=1  fixtures + dev holdout + invariants only (quick iteration)
#   META=1  adds metamorphic runs on d1 (shuffle, split, headers, requote, reencode, all)
#   FUZZ=1  adds byte-level fuzz vs reference.py on seeds 1-50 (scripts/fuzz.py)
#   BIG=1   adds the big set and TIME line
# PIPE overrides the command under test, e.g. PIPE=native/target/release/shopping-sessions.exe
# ORACLE_ROOT overrides the dir that scripts/oracle.sha256 paths are relative to (testing the guard).
# Every run appends a line to bench/verify.log.
set -u
PIPE="${PIPE:-python pipeline/pipeline.py}"
cd "$(dirname "$0")/.." || { echo "FAIL setup: cannot cd to project"; exit 1; }
LOG=$(mktemp)
START=$(date +%s)
STEP=setup
RESULT=""
if [ "${FAST:-0}" = 1 ]; then MODE=fast; else MODE=full; fi
[ "${META:-0}" = 1 ] && MODE="$MODE+meta"
[ "${BIG:-0}" = 1 ] && MODE="$MODE+big"
[ "${FUZZ:-0}" = 1 ] && MODE="$MODE+fuzz"
finish() {
  local rc=$? commit dirty
  rm -f "$LOG"
  [ -z "$RESULT" ] && { [ "$rc" -eq 0 ] && RESULT=PASS || RESULT="FAIL:$STEP"; }
  commit=$(git rev-parse --short HEAD 2>/dev/null || echo none)
  if [ -n "$(git --no-optional-locks status --porcelain -- pipeline native 2>/dev/null)" ]; then dirty=1; else dirty=0; fi
  mkdir -p bench
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date -u +%FT%TZ)" "$commit" "$dirty" "$PIPE" "$MODE" "$RESULT" \
    "$(( $(date +%s) - START ))" >> bench/verify.log
}
trap finish EXIT
fail() { RESULT="FAIL:${2:-$STEP}"; echo "FAIL $1"; tail -n 40 "$LOG"; exit 1; }
absdir() { (cd "$1" && pwd -W 2>/dev/null || pwd); }

# oracle guard: reference.py, compare.py, run_fixtures.py and fixtures must be untouched
STEP=oracle
(cd "${ORACLE_ROOT:-.}" && sha256sum -c --quiet scripts/oracle.sha256) >"$LOG" 2>&1 || fail "oracle: changed"
echo "PASS oracle: unchanged"

# a. fixtures: last line must be the full count
STEP=fixtures
python scripts/run_fixtures.py fixtures -- $PIPE >"$LOG" 2>/dev/null
last=$(tail -n 1 "$LOG")
[ "$last" = "36/36 fixtures passed" ] || fail "fixtures: last line '$last'"
echo "PASS fixtures: $last"

# a2. regression cases from review findings (same layout as fixtures): all modes
STEP=regress
python scripts/run_fixtures.py regress -- $PIPE >"$LOG" 2>/dev/null
last=$(tail -n 1 "$LOG")
[ "$last" = "4/4 fixtures passed" ] || fail "regress: last line '$last'"
echo "PASS regress: $last"

# run_pipe NAME DATA OUT: run PIPE with stderr kept in $LOG; prints TIME when TIMED=1
run_pipe() {
  local name=$1 data=$2 out=$3 t0 t1 ms t
  rm -rf "$out"; mkdir -p "$out"
  t0=$(date +%s%N)
  DATA_DIR="$data" OUTPUT_DIR="$out" $PIPE >/dev/null 2>"$LOG"; local rc=$?
  t1=$(date +%s%N)
  [ "$rc" -eq 0 ] || fail "pipeline $name (exit $rc)"
  if [ "${TIMED:-0}" = 1 ]; then
    ms=$(( (t1 - t0) / 1000000 )); t=$(( ms / 100 ))
    echo "TIME $name $((t / 10)).$((t % 10))"
  fi
}
inv() { # inv NAME OUT MANIFEST
  python scripts/check_inv.py "$2" "$3" >"$LOG" 2>&1 || fail "inv-$1: $(tail -n 1 "$LOG")" "inv-$1"
  echo "PASS inv-$1"
}
cmp_out() { # cmp_out NAME A B
  local res last
  res=$(cd scripts && python compare.py "$2" "$3" 2>&1); printf '%s\n' "$res" >"$LOG"
  last=$(printf '%s\n' "$res" | tail -n 1)
  [ "$last" = "MATCH" ] || fail "compare $1: last line '$last'" "$1"
}

STRESS="${STRESS:-../../stress}"
mkdir -p "$STRESS" || { echo "FAIL setup: cannot create $STRESS"; exit 1; }
STRESS=$(absdir "$STRESS")

# b. official dev holdout vs official answer, plus invariants
STEP=dev
DEV="${DEV:-../../dev}"; ANS="${ANS:-../../ans/dev-answer}"
if [ -d "$DEV" ] && [ -d "$ANS" ]; then
  DEV=$(absdir "$DEV"); ANS=$(absdir "$ANS")
  TIMED=1 run_pipe dev "$DEV" "$STRESS/devpol"
  cmp_out dev "$ANS" "$STRESS/devpol"
  echo "PASS dev: MATCH"
  inv dev "$STRESS/devpol" "$DEV/manifest.json"
else
  echo "SKIP dev: $DEV or $ANS missing"
fi

# diffset NAME DATA REF POL SEED  (SEED empty = big set: never generated, SKIP if data missing)
diffset() {
  local name=$1 data=$2 ref=$3 pol=$4 seed=$5
  STEP=$name
  if [ ! -d "$data" ]; then
    [ -z "$seed" ] && { echo "SKIP $name: $data missing"; return 0; }
    python scripts/stress_gen.py "$data" --seed "$seed" --events 200000 >"$LOG" 2>&1 || fail "generate $name"
  fi
  if [ ! -d "$ref" ]; then
    DATA_DIR="$data" OUTPUT_DIR="$ref" python pipeline/reference.py >"$LOG" 2>&1 || fail "reference $name"
  fi
  [ -z "$seed" ] && export TIMED=1
  run_pipe "$name" "$data" "$pol"
  unset TIMED
  cmp_out "$name" "$ref" "$pol"
  echo "PASS $name: MATCH"
  inv "$name" "$pol" "$data/manifest.json"
}

# metamorphic: transformed d1 must give the same output as untransformed d1 (pipeline only, no oracle)
meta_set() {
  local d1="$STRESS/d1" base="$STRESS/metabase" rel dst
  [ -d "$d1" ] || python scripts/stress_gen.py "$d1" --seed 1 --events 200000 >"$LOG" 2>&1 || fail "generate d1" meta
  STEP=meta-base
  run_pipe meta-base "$d1" "$base"
  for rel in shuffle split headers requote reencode all; do
    STEP="meta-$rel"; dst="$STRESS/meta-d1"
    rm -rf "$dst" "$STRESS/metaout"
    python scripts/meta.py "$d1" "$dst" --relation "$rel" --seed "${META_SEED:-1}" >"$LOG" 2>&1 || fail "meta.py $rel"
    run_pipe "meta-$rel" "$dst" "$STRESS/metaout"
    cmp_out "meta-$rel" "$base" "$STRESS/metaout"
    echo "PASS meta-$rel: MATCH"
  done
  rm -rf "$STRESS/meta-d1" "$STRESS/metaout"
}

if [ "${FAST:-0}" != 1 ]; then
  for seed in 1 2 3; do
    diffset "seed$seed" "$STRESS/d$seed" "$STRESS/ref$seed" "$STRESS/pol$seed" "$seed"
  done
  if [ "${BIG:-0}" = 1 ]; then
    diffset big "$STRESS/big" "$STRESS/bigref" "$STRESS/bigout" ""
  else
    echo "SKIP big: BIG=1 not set"
  fi
else
  echo "SKIP seeds: FAST=1"
fi
if [ "${META:-0}" = 1 ]; then meta_set; else echo "SKIP meta: META=1 not set"; fi

# fuzz: byte-level CSV/timestamp fuzz vs reference.py, seeds 1-50 (optional)
if [ "${FUZZ:-0}" = 1 ]; then
  STEP=fuzz
  python scripts/fuzz.py --pipe "$PIPE" --seeds 1-50 --workdir "$STRESS/fuzz" >"$LOG" 2>&1 || fail "fuzz: $(grep -m1 'FUZZ FAIL' "$LOG")" fuzz
  echo "PASS fuzz: 50 seeds"
else
  echo "SKIP fuzz: FUZZ=1 not set"
fi

STEP=done
echo "VERIFY PASS"
