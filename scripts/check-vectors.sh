#!/usr/bin/env bash
# When flags-stack is checked out at .flags-stack (CI's vectors job does it, at .flags-stack-rev),
# the vendored vectors must be exactly flags-stack's. Without the checkout there's nothing to compare.
set -euo pipefail
if [ ! -d .flags-stack/protocol/vectors/eval ]; then
  echo "check-vectors: no .flags-stack checkout; skipped"
  exit 0
fi
diff -r --exclude SOURCE .flags-stack/protocol/vectors/eval test/vectors
echo "check-vectors: test/vectors match .flags-stack/protocol/vectors/eval"
