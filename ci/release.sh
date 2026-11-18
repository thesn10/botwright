#!/usr/bin/env bash
set -euo pipefail

changelog=CHANGELOG.md

section() {
  awk -v header="## [$1]" 'index($0, header) == 1 { found = 1; next } /^## \[/ { found = 0 } found' "$changelog"
}

package_version() {
  cargo metadata --no-deps --format-version 1 | jq -r '.packages[] | select(.name == "botwright") | .version'
}

next_version() {
  local current=$1 bump=$2 major minor patch
  if [[ $bump =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
    echo "$bump"
    return
  fi
  if [[ $current == *-* ]]; then
    echo "current version $current is a pre-release; pass the exact next version" >&2
    exit 1
  fi
  IFS=. read -r major minor patch <<<"$current"
  case $bump in
    major) echo "$((major + 1)).0.0" ;;
    minor) echo "$major.$((minor + 1)).0" ;;
    patch) echo "$major.$minor.$((patch + 1))" ;;
    *)
      echo "expected patch, minor, major or a version, got '$bump'" >&2
      exit 1
      ;;
  esac
}

check() {
  if ! section Unreleased | grep -q '[^[:space:]]'; then
    echo "$changelog has nothing under ## [Unreleased]" >&2
    exit 1
  fi
}

prepare() {
  check
  local current next
  current=$(package_version)
  next=$(next_version "$current" "$1")
  sed -i "0,/^version = \".*\"\$/s//version = \"$next\"/" Cargo.toml
  cargo metadata --format-version 1 >/dev/null
  if [[ $(package_version) != "$next" ]]; then
    echo "failed to set version $next in Cargo.toml" >&2
    exit 1
  fi
  sed -i "s/^## \[Unreleased\]\$/## [Unreleased]\n\n## [$next] - $(date -u +%F)/" "$changelog"
  echo "$next"
}

notes() {
  section "$1" | sed '/[^[:space:]]/,$!d'
}

case ${1:-} in
  check) check ;;
  prepare) prepare "${2:?bump or version required}" ;;
  notes) notes "${2:?version required}" ;;
  *)
    echo "usage: $0 check | prepare <patch|minor|major|X.Y.Z> | notes <X.Y.Z>" >&2
    exit 1
    ;;
esac
