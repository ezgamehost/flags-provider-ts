#!/usr/bin/env bash
# Copies flags-stack's conformance vectors (protocol/vectors/eval) verbatim into test/vectors and
# records the commit in .flags-stack-rev.
#   scripts/sync-vectors.sh ~/code/flags-stack [<commit>]
# With a commit, the vectors come from that commit (git show), not the checkout's files.
set -euo pipefail
src=${1:?usage: scripts/sync-vectors.sh <flags-stack checkout> [<commit>]}
rev=$(git -C "$src" rev-parse "${2:-HEAD}")
mkdir -p test/vectors
rm -f test/vectors/*.json
for f in $(git -C "$src" ls-tree --name-only "$rev" protocol/vectors/eval/ | grep '\.json$'); do
  git -C "$src" show "$rev:$f" > "test/vectors/$(basename "$f")"
done
echo "ezgamehost/flags-stack protocol/vectors/eval at $rev (verbatim; change them there)" > test/vectors/SOURCE
echo "$rev" > .flags-stack-rev
