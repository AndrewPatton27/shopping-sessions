#!/usr/bin/env bash
# Copy the shared team log (../../context) into this repo before a commit, so the
# GitHub history carries the provenance trail. Research notes stay local.
set -eu
cd "$(dirname "$0")/.."
mkdir -p context
for f in STATUS.md DECISIONS.md CHANGELOG.md HANDOFF.md FAILURES.md; do
  cp "../../context/$f" "context/$f"
done
echo "synced context: $(ls context | tr '\n' ' ')"
