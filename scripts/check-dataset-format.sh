#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
fixture_root=$(mktemp -d /tmp/zsync-dataset-check.XXXXXX)
trap 'rm -rf "$fixture_root"' EXIT

cd "$repo_root"

go test ./...
go vet ./...
zig build

for fixture_kind in linear reorg; do
  fixture_path="$fixture_root/$fixture_kind"
  go run ./cmd/zsync-dataset fixture "$fixture_kind" "$fixture_path"
  go run ./cmd/zsync-dataset validate "$fixture_path" >/dev/null
  go run ./cmd/zsync-dataset events "$fixture_path" >"$fixture_root/go-$fixture_kind.txt"
  zig build run -- "$fixture_path" >"$fixture_root/zig-$fixture_kind.txt"
  diff -u "$fixture_root/go-$fixture_kind.txt" "$fixture_root/zig-$fixture_kind.txt"
done

echo "dataset format checks passed"
