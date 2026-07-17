#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VERSION=${1:-}
OUT_DIR=${2:-"$ROOT/dist"}
REQUESTED_ARCH=${3:-}
IDENTIFIER=org.openclaw.gitcrawl
EXPECTED_AUTHORITY='Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)'
EXPECTED_TEAM_ID=FWJYW4S8P8
REQUIREMENT="identifier \"$IDENTIFIER\" and anchor apple generic and certificate 1[field.1.2.840.113635.100.6.2.6] exists and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = \"$EXPECTED_TEAM_ID\""

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]] ||
  [[ -n "$REQUESTED_ARCH" && "$REQUESTED_ARCH" != amd64 && "$REQUESTED_ARCH" != arm64 ]]; then
  echo "usage: $0 vX.Y.Z [artifact-directory] [amd64|arm64]" >&2
  exit 2
fi
[[ "$(uname -s)" == Darwin ]] || {
  echo "macOS release verification must run on macOS" >&2
  exit 1
}

checksums="$OUT_DIR/checksums.txt"
[[ -f "$checksums" ]] || {
  echo "missing checksums: $checksums" >&2
  exit 1
}

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/gitcrawl-release-verify.XXXXXX")
trap 'rm -rf "$WORK_DIR"' EXIT
release_version=${VERSION#v}
architectures=(amd64 arm64)
[[ -z "$REQUESTED_ARCH" ]] || architectures=("$REQUESTED_ARCH")

for goarch in "${architectures[@]}"; do
  archive_name="gitcrawl_${release_version}_darwin_${goarch}.tar.gz"
  archive="$OUT_DIR/$archive_name"
  [[ -f "$archive" ]] || {
    echo "missing release archive: $archive" >&2
    exit 1
  }

  checksum_record=$(awk -v name="$archive_name" '$2 == name { print }' "$checksums")
  [[ "$(printf '%s\n' "$checksum_record" | awk 'NF { count++ } END { print count+0 }')" == 1 ]] || {
    echo "missing or duplicate checksum record: $archive_name" >&2
    exit 1
  }
  read -r expected_hash expected_name extra <<<"$checksum_record"
  [[ "$expected_hash" =~ ^[[:xdigit:]]{64}$ && "$expected_name" == "$archive_name" && -z "${extra:-}" ]] || {
    echo "invalid checksum record: $archive_name" >&2
    exit 1
  }
  actual_hash=$(shasum -a 256 "$archive" | awk '{print $1}')
  [[ "$actual_hash" == "$expected_hash" ]] || {
    echo "checksum mismatch: $archive" >&2
    exit 1
  }

  expected_listing=$'CHANGELOG.md\nLICENSE\nREADME.md\ngitcrawl'
  actual_listing=$(tar -tzf "$archive" | LC_ALL=C sort)
  [[ "$actual_listing" == "$expected_listing" ]] || {
    echo "unexpected archive contents: $archive" >&2
    exit 1
  }

  stage="$WORK_DIR/$goarch"
  mkdir -p "$stage"
  tar -xzf "$archive" -C "$stage"
  binary="$stage/gitcrawl"
  [[ -x "$binary" ]] || {
    echo "missing executable in archive: $archive" >&2
    exit 1
  }

  codesign --verify --strict -R="$REQUIREMENT" --verbose=2 "$binary"
  codesign --verify --strict --check-notarization -R=notarized "$binary"
  signature=$(codesign -dvvv "$binary" 2>&1)
  grep -Fx "Identifier=$IDENTIFIER" <<<"$signature" >/dev/null
  grep -Fx "TeamIdentifier=$EXPECTED_TEAM_ID" <<<"$signature" >/dev/null
  grep -Fx "Authority=$EXPECTED_AUTHORITY" <<<"$signature" >/dev/null

  expected_arch=$goarch
  [[ "$goarch" == amd64 ]] && expected_arch=x86_64
  lipo -archs "$binary" | tr ' ' '\n' | grep -Fx "$expected_arch" >/dev/null
  [[ "$("$binary" --version)" == "$release_version" ]]
done
