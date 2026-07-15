#!/usr/bin/env bash
set -euo pipefail

binary=${1:-}
require_signing=${GITCRAWL_REQUIRE_CODESIGN:-0}
identifier=org.openclaw.gitcrawl
expected_team_id=FWJYW4S8P8
expected_authority="Developer ID Application: OpenClaw Foundation ($expected_team_id)"
requirement="identifier \"$identifier\" and anchor apple generic and certificate 1[field.1.2.840.113635.100.6.2.6] exists and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate leaf[subject.OU] = \"$expected_team_id\""

if [[ -z "$binary" ]]; then
  echo "usage: $0 <path-to-binary>" >&2
  exit 2
fi
if [[ ! -f "$binary" ]]; then
  echo "codesign: binary not found: $binary" >&2
  exit 2
fi

if [[ "$require_signing" != 1 ]]; then
  exit 0
fi

[[ "$(uname -s)" == Darwin ]] || {
  echo "codesign: official macOS release signing requires a macOS runner" >&2
  exit 1
}

identity=${CODESIGN_IDENTITY:-}
[[ "$identity" == "$expected_authority" ]] || {
  echo "codesign: official macOS releases require $expected_authority" >&2
  exit 1
}

codesign --force \
  --options runtime \
  --timestamp \
  --identifier "$identifier" \
  --sign "$identity" \
  "$binary"
codesign --verify --strict -R="$requirement" --verbose=2 "$binary"

signature=$(codesign -dvvv "$binary" 2>&1)
grep -Fx "Identifier=$identifier" <<<"$signature" >/dev/null
grep -Fx "TeamIdentifier=$expected_team_id" <<<"$signature" >/dev/null
grep -Fx "Authority=$expected_authority" <<<"$signature" >/dev/null
