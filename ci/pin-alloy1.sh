#!/usr/bin/env bash
# Pins every alloy 1.x crate in Cargo.lock to one version; alloy-core crates keep their own versions.
set -euo pipefail
version=$1
core='^alloy-(primitives|sol|json-abi|dyn-abi|rlp|trie|core|hardforks|chains|eip[0-9]|rpc-types-(debug|trace|anvil|engine|txpool|admin))'
locked() {
  awk -v want="$version" '
    /^\[\[package\]\]/ { name = "" }
    /^name = "alloy/ { name = $3; gsub(/"/, "", name) }
    /^version = "1\./ { v = $3; gsub(/"/, "", v); if (name != "" && v != want) print name "@" v }
  ' Cargo.lock | grep -v -E "$core" | sort -u || true
}
for _ in $(seq 20); do
  pending=$(locked)
  [ -z "$pending" ] && break
  for spec in $pending; do
    cargo update --quiet -p "$spec" --precise "$version" 2>/dev/null || true
  done
done
remaining=$(locked)
if [ -n "$remaining" ]; then
  echo "could not pin: $remaining" >&2
  exit 1
fi
