#!/usr/bin/env bash
# Verify the Shopping Sessions pipeline: fixtures, then differential test vs reference.py.
# Run from starter/starter-duckdb: bash scripts/verify.sh   (BIG=1 adds the big set and TIME line)
# PIPE overrides the command under test, e.g. PIPE=native/target/release/shopping-sessions.exe
set -u
PIPE="${PIPE:-python pipeline/pipeline.py}"
cd "$(dirname "$0")/.." || { echo "FAIL setup: cannot cd to project"; exit 1; }
LOG=$(mktemp)
trap 'rm -f "$LOG"' EXIT
fail() { echo "FAIL $1"; tail -n 40 "$LOG"; exit 1; }
absdir() { (cd "$1" && pwd -W 2>/dev/null || pwd); }

# a. fixtures: last line must be the full count
python scripts/run_fixtures.py fixtures -- $PIPE >"$LOG" 2>/dev/null
last=$(tail -n 1 "$LOG")
[ "$last" = "36/36 fixtures passed" ] || fail "fixtures: last line '$last'"
echo "PASS fixtures: $last"

# b/c. diffset NAME DATA REF POL SEED  (SEED empty = big set: never generated, SKIP if data missing)
diffset() {
  local name=$1 data=$2 ref=$3 pol=$4 seed=$5 t0 t1 ms t res last
  if [ ! -d "$data" ]; then
    [ -z "$seed" ] && { echo "SKIP $name: $data missing"; return 0; }
    python scripts/stress_gen.py "$data" --seed "$seed" --events 200000 >"$LOG" 2>&1 || fail "generate $name"
  fi
  if [ ! -d "$ref" ]; then
    DATA_DIR="$data" OUTPUT_DIR="$ref" python pipeline/reference.py >"$LOG" 2>&1 || fail "reference $name"
  fi
  rm -rf "$pol"; mkdir -p "$pol"
  t0=$(date +%s%N)
  DATA_DIR="$data" OUTPUT_DIR="$pol" $PIPE >/dev/null 2>/dev/null || fail "pipeline $name (exit $?)"
  t1=$(date +%s%N)
  if [ -z "$seed" ]; then
    ms=$(( (t1 - t0) / 1000000 )); t=$(( ms / 100 ))
    echo "TIME $name $((t / 10)).$((t % 10))"
  fi
  res=$(cd scripts && python compare.py "$ref" "$pol" 2>&1); printf '%s\n' "$res" >"$LOG"
  last=$(printf '%s\n' "$res" | tail -n 1)
  [ "$last" = "MATCH" ] || fail "compare $name: last line '$last'"
  echo "PASS $name: MATCH"
}

STRESS="${STRESS:-../../stress}"
mkdir -p "$STRESS" || { echo "FAIL setup: cannot create $STRESS"; exit 1; }
STRESS=$(absdir "$STRESS")
for seed in 1 2 3; do
  diffset "seed$seed" "$STRESS/d$seed" "$STRESS/ref$seed" "$STRESS/pol$seed" "$seed"
done
if [ "${BIG:-0}" = 1 ]; then
  diffset big "$STRESS/big" "$STRESS/bigref" "$STRESS/bigout" ""
else
  echo "SKIP big: BIG=1 not set"
fi

echo "VERIFY PASS"
