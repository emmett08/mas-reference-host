#!/usr/bin/env bash
# Fetch the reviewed external source; never overwrite an existing checkout.
set -euo pipefail
root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
revision="$(cat protocol-revision.txt)"
[[ "$revision" =~ ^[0-9a-f]{40}$ ]] || { echo 'Expected a full protocol commit SHA.' >&2; exit 1; }
if [[ ! -e .protocol ]]; then
  git init -q .protocol
  git -C .protocol remote add origin https://github.com/emmett08/mas.git
  git -C .protocol fetch --depth 1 origin "$revision"
  git -C .protocol checkout --detach FETCH_HEAD
fi
[[ "$(git -C .protocol rev-parse HEAD)" == "$revision" ]] || {
  echo 'Existing .protocol has a different revision; move it aside before preparing this pin.' >&2
  exit 1
}
git -C .protocol diff --exit-code HEAD --
printf '%s\n' "$revision" > .protocol-revision
python3 scripts/check-protocol-fixtures.py
