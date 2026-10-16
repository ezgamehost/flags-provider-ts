#!/usr/bin/env bash
# One-time owner setup for npm trusted publishing (run locally, logged in to npm with 2FA; npm 11.15+).
# npm can only trust a package that already exists, so for each package name given that isn't on
# the registry yet this publishes an empty, deprecated 0.0.0 placeholder first. It then trusts this
# repository's .github/workflows/release.yml to publish (OIDC, no tokens) and sets the package to
# require 2FA and disallow tokens. Safe to re-run: existing packages and trust entries are kept.
#
#   scripts/npm-trust.sh @ezghcloud/foo [@ezghcloud/bar ...]
set -euo pipefail

[ $# -gt 0 ] || { echo "usage: $0 <package>..." >&2; exit 2; }
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
WORKFLOW=release.yml
npm whoami >/dev/null || { echo "run npm login first" >&2; exit 1; }

for name in "$@"; do
  if ! npm view "$name" name >/dev/null 2>&1; then
    dir=$(mktemp -d)
    cat >"$dir/package.json" <<EOF
{
  "name": "$name",
  "version": "0.0.0",
  "description": "Placeholder: releases are published from https://github.com/$REPO",
  "repository": { "type": "git", "url": "git+https://github.com/$REPO.git" },
  "license": "UNLICENSED"
}
EOF
    echo "Releases of $name are published from https://github.com/$REPO." >"$dir/README.md"
    (cd "$dir" && npm publish --access public)
    rm -rf "$dir"
  fi
  # A new package takes a little while to become writable after its first publish.
  for _ in $(seq 30); do npm view "$name@0.0.0" deprecated --json >/dev/null 2>&1 && break; sleep 5; done
  [ -n "$(npm view "$name@0.0.0" deprecated 2>/dev/null)" ] ||
    npm deprecate "$name@0.0.0" "Placeholder; install a later version"
  # npm allows one trust entry per package; if one exists this fails and `npm trust list` shows it.
  npm trust github "$name" --repo "$REPO" --file "$WORKFLOW" --allow-publish --yes ||
    { echo "$name: not trusted now; check npm trust list $name" >&2; npm trust list "$name" || true; }
  npm access set mfa=publish "$name"
  sleep 2
done
