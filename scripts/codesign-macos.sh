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
[[ -n "${NOTARYTOOL_KEYCHAIN_PROFILE:-}" ]] || {
  echo "codesign: NOTARYTOOL_KEYCHAIN_PROFILE is required at runtime" >&2
  exit 1
}

for tool in codesign ditto mktemp mv plutil xcrun; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "codesign: missing required command: $tool" >&2
    exit 1
  }
done

binary_dir=$(cd "$(dirname "$binary")" && pwd)
binary_name=$(basename "$binary")
work_dir=$(mktemp -d "$binary_dir/.gitcrawl-notary.XXXXXX")
candidate="$work_dir/$binary_name"
submission="$work_dir/$binary_name.zip"
trap 'rm -rf "$work_dir"' EXIT

# Keep the GoReleaser output unchanged unless the complete signing and
# notarization contract succeeds.
cp -p "$binary" "$candidate"
codesign --force \
  --options runtime \
  --timestamp \
  --identifier "$identifier" \
  --sign "$identity" \
  "$candidate"
codesign --verify --strict -R="$requirement" --verbose=2 "$candidate"

signature=$(codesign -dvvv "$candidate" 2>&1)
grep -Fx "Identifier=$identifier" <<<"$signature" >/dev/null
grep -Fx "TeamIdentifier=$expected_team_id" <<<"$signature" >/dev/null
grep -Fx "Authority=$expected_authority" <<<"$signature" >/dev/null

ditto -c -k --sequesterRsrc --keepParent "$candidate" "$submission"
notary_result=$(xcrun notarytool submit "$submission" \
  --keychain-profile "$NOTARYTOOL_KEYCHAIN_PROFILE" \
  --no-s3-acceleration \
  --wait \
  --output-format json)
notary_status=$(plutil -extract status raw -o - - <<<"$notary_result")
notary_id=$(plutil -extract id raw -o - - <<<"$notary_result")
[[ "$notary_status" == Accepted ]] || {
  echo "codesign: notarization status is ${notary_status:-missing}, expected Accepted" >&2
  exit 1
}
[[ "$notary_id" =~ ^[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}$ ]] || {
  echo "codesign: notarization response has an invalid submission id" >&2
  exit 1
}

codesign --verify --strict --check-notarization -R=notarized "$candidate"
mv -f "$candidate" "$binary"
