#!/usr/bin/env bash
# Usage: bash scripts/bench.sh NAME [DATA_DIR]
# Env: PIPE (command under test, default "python pipeline/pipeline.py"), RUNS (default 3)
# Run from repo root. Appends one row to bench/results.tsv.
set -u

NAME=${1:?usage: bash scripts/bench.sh NAME [DATA_DIR]}
DATA=${2:-../../stress/big}
PIPE=${PIPE:-python pipeline/pipeline.py}
RUNS=${RUNS:-3}
BIG=../../stress/big
REF=../../stress/bigref
OUT_ROOT=../../stress/bench-out/$NAME-$(date -u +%Y%m%dT%H%M%S)
RESULTS=bench/results.tsv

abspath() { (cd "$1" && pwd -W 2>/dev/null || pwd); }
fmt() { local c=$(( ($1 + 5) / 10 )); printf '%d.%02d' $((c / 100)) $((c % 100)); }

mkdir -p "$OUT_ROOT"
times=()
last_out=""
for i in $(seq 1 "$RUNS"); do
  out="$OUT_ROOT/run$i/out"
  log="$OUT_ROOT/run$i.log"
  mkdir -p "$out"
  t0=$(date +%s%N)
  DATA_DIR="$DATA" OUTPUT_DIR="$out" $PIPE > "$log" 2>&1
  rc=$?
  t1=$(date +%s%N)
  if [ "$rc" -ne 0 ]; then echo "run $i failed rc=$rc, see $log" >&2; exit 1; fi
  times+=($(( (t1 - t0) / 1000000 )))
  last_out=$out
done

sorted=($(printf '%s\n' "${times[@]}" | sort -n))
n=${#sorted[@]}
min=${sorted[0]}
max=${sorted[$((n - 1))]}
if [ $((n % 2)) -eq 1 ]; then
  med=${sorted[$((n / 2))]}
else
  med=$(( (sorted[n / 2 - 1] + sorted[n / 2]) / 2 ))
fi

tag=NA
if [ -d "$REF" ] && [ "$(abspath "$DATA")" = "$(abspath "$BIG")" ]; then
  if (cd scripts && python compare.py "$(abspath "$REF")" "$(abspath "$last_out")") > "$OUT_ROOT/compare.log" 2>&1; then
    tag=MATCH
  else
    tag=DIFFER
  fi
fi

commit=$(git rev-parse --short HEAD 2>/dev/null || echo none)
if [ -n "$(git status --porcelain -- pipeline native 2>/dev/null)" ]; then dirty=1; else dirty=0; fi
ts=$(date -u +%FT%TZ)
dataset=$(basename "$DATA")

if [ ! -f "$RESULTS" ]; then
  printf 'ts\tname\tcommit\tdirty\tdataset\truns\tmedian_s\tmin_s\tmax_s\tcorrect\tpipe\n' > "$RESULTS"
fi
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  "$ts" "$NAME" "$commit" "$dirty" "$dataset" "$RUNS" \
  "$(fmt "$med")" "$(fmt "$min")" "$(fmt "$max")" "$tag" "$PIPE" >> "$RESULTS"

echo "BENCH $NAME median=$(fmt "$med")s min=$(fmt "$min")s max=$(fmt "$max")s correct=$tag"
