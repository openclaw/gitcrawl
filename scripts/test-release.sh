#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
EXPECTED_AUTHORITY='Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)'

for script in codesign-macos.sh package-release.sh verify-release.sh check-macos-minimum.sh; do
  bash -n "$ROOT/scripts/$script"
done

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/gitcrawl-release-test.XXXXXX")
trap 'rm -rf "$WORK_DIR"' EXIT
FAKE_BIN="$WORK_DIR/bin"
mkdir -p "$FAKE_BIN"

cat > "$FAKE_BIN/uname" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
  -s) echo Darwin ;;
  -m) echo arm64 ;;
  *) echo Darwin ;;
esac
EOF

cat > "$FAKE_BIN/codesign" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "${MOCK_CODESIGN_LOG:?}"
if [[ " $* " == *' --check-notarization '* && "${MOCK_NOTARIZATION_REJECT:-0}" == 1 ]]; then
  exit 1
fi
case " $* " in
  *' -dvvv '*)
    {
      echo 'Identifier=org.openclaw.gitcrawl'
      echo "Authority=${MOCK_CODESIGN_AUTHORITY:-Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)}"
      echo "TeamIdentifier=${MOCK_CODESIGN_TEAM_ID:-FWJYW4S8P8}"
    } >&2
    ;;
esac
EOF

cat > "$FAKE_BIN/ditto" <<'EOF'
#!/usr/bin/env bash
previous=
for arg in "$@"; do
  source=$previous
  previous=$arg
done
cp "$source" "$previous"
printf 'ditto %s\n' "$*" >> "${MOCK_CODESIGN_LOG:?}"
EOF

cat > "$FAKE_BIN/xcrun" <<'EOF'
#!/usr/bin/env bash
printf 'xcrun %s\n' "$*" >> "${MOCK_CODESIGN_LOG:?}"
printf '{"id":"12345678-1234-1234-1234-123456789abc","status":"%s"}\n' "${MOCK_NOTARY_STATUS:-Accepted}"
EOF

cat > "$FAKE_BIN/plutil" <<'EOF'
#!/usr/bin/env bash
case "${2:-}" in
  status) printf '%s\n' "${MOCK_NOTARY_STATUS:-Accepted}" ;;
  id) printf '%s\n' '12345678-1234-1234-1234-123456789abc' ;;
  *) exit 1 ;;
esac
EOF

cat > "$FAKE_BIN/lipo" <<'EOF'
#!/usr/bin/env bash
case "${2:-}" in
  */amd64/*) echo x86_64 ;;
  *) echo arm64 ;;
esac
EOF

cat > "$FAKE_BIN/otool" <<'EOF'
#!/usr/bin/env bash
printf '      cmd LC_BUILD_VERSION\n  cmdsize 32\n platform 1\n    minos %s\n' "${MOCK_MACOS_MINIMUM:-13.0}"
EOF

cat > "$FAKE_BIN/arch" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
requested=$1
shift
printf '%s %s\n' "$requested" "$*" >> "${MOCK_ARCH_LOG:?}"
if [[ "$requested" == -x86_64 && "${MOCK_ROSETTA_MISSING:-0}" == 1 && ! -f "${MOCK_ROSETTA_READY:?}" ]]; then
  exit 1
fi
if [[ "$1" == /usr/bin/true ]]; then
  exit 0
fi
case "$1" in
  */amd64/gitcrawl) [[ "$requested" == -x86_64 ]] ;;
  */arm64/gitcrawl) [[ "$requested" == -arm64 ]] ;;
  *) exit 2 ;;
esac
exec "$@"
EOF

cat > "$FAKE_BIN/sudo" <<'EOF'
#!/usr/bin/env bash
exec "$@"
EOF

cat > "$FAKE_BIN/softwareupdate" <<'EOF'
#!/usr/bin/env bash
[[ "$*" == '--install-rosetta --agree-to-license' ]] || exit 2
printf '%s\n' "$*" >> "${MOCK_ROSETTA_LOG:?}"
touch "${MOCK_ROSETTA_READY:?}"
EOF

chmod 0755 "$FAKE_BIN"/*
export PATH="$FAKE_BIN:$PATH"
export MOCK_CODESIGN_LOG="$WORK_DIR/codesign.log"
export MOCK_ARCH_LOG="$WORK_DIR/arch.log"
export MOCK_ROSETTA_LOG="$WORK_DIR/rosetta.log"
export MOCK_ROSETTA_READY="$WORK_DIR/rosetta-ready"
unset NOTARYTOOL_KEYCHAIN_PROFILE

test_binary="$WORK_DIR/gitcrawl"
cat > "$test_binary" <<'EOF'
#!/usr/bin/env bash
[[ "${1:-}" == --version ]] || exit 2
echo 0.7.1
EOF
chmod 0755 "$test_binary"

"$ROOT/scripts/check-macos-minimum.sh" "$test_binary"
if MOCK_MACOS_MINIMUM=14.0 "$ROOT/scripts/check-macos-minimum.sh" "$test_binary" >/dev/null 2>&1; then
  echo "unsupported macOS deployment target was accepted" >&2
  exit 1
fi
if MOCK_MACOS_MINIMUM=missing "$ROOT/scripts/check-macos-minimum.sh" "$test_binary" >/dev/null 2>&1; then
  echo "missing macOS deployment target was accepted" >&2
  exit 1
fi

GITCRAWL_REQUIRE_CODESIGN=0 "$ROOT/scripts/codesign-macos.sh" "$test_binary"
if package_output=$("$ROOT/scripts/package-release.sh" 2>&1); then
  echo "local package script unexpectedly succeeded" >&2
  exit 1
fi
grep -F 'gh workflow run release-unified.yml --repo openclaw/gitcrawl -f version=X.Y.Z' <<<"$package_output" >/dev/null
if GITCRAWL_REQUIRE_CODESIGN=1 \
  CODESIGN_IDENTITY='Developer ID Application: Peter Steinberger (Y5PE65HELJ)' \
  "$ROOT/scripts/codesign-macos.sh" "$test_binary" >/dev/null 2>&1; then
  echo "personal signing identity was accepted" >&2
  exit 1
fi
if GITCRAWL_REQUIRE_CODESIGN=1 \
  CODESIGN_IDENTITY="$EXPECTED_AUTHORITY" \
  "$ROOT/scripts/codesign-macos.sh" "$test_binary" >/dev/null 2>&1; then
  echo "official signing accepted a missing notary profile" >&2
  exit 1
fi
GITCRAWL_REQUIRE_CODESIGN=1 \
  CODESIGN_IDENTITY="$EXPECTED_AUTHORITY" \
  NOTARYTOOL_KEYCHAIN_PROFILE=test-profile \
  "$ROOT/scripts/codesign-macos.sh" "$test_binary"
grep -F -- '--identifier org.openclaw.gitcrawl' "$MOCK_CODESIGN_LOG" >/dev/null
grep -F 'FWJYW4S8P8' "$MOCK_CODESIGN_LOG" >/dev/null
grep -F -- 'notarytool submit' "$MOCK_CODESIGN_LOG" >/dev/null
grep -F -- '--keychain-profile test-profile --no-s3-acceleration --wait --output-format json' "$MOCK_CODESIGN_LOG" >/dev/null
signed_hash=$(shasum -a 256 "$test_binary" | awk '{ print $1 }')
if GITCRAWL_REQUIRE_CODESIGN=1 \
  CODESIGN_IDENTITY="$EXPECTED_AUTHORITY" \
  NOTARYTOOL_KEYCHAIN_PROFILE=test-profile \
  MOCK_NOTARY_STATUS=Invalid \
  "$ROOT/scripts/codesign-macos.sh" "$test_binary" >/dev/null 2>&1; then
  echo "invalid notarization status was accepted" >&2
  exit 1
fi
[[ "$(shasum -a 256 "$test_binary" | awk '{ print $1 }')" == "$signed_hash" ]] || {
  echo "failed notarization mutated the release binary" >&2
  exit 1
}
if find "$WORK_DIR" -maxdepth 1 -name '.gitcrawl-notary.*' | grep -q .; then
  echo "ephemeral notarization files were not removed" >&2
  exit 1
fi

ARTIFACTS="$WORK_DIR/artifacts"
mkdir -p "$ARTIFACTS"
for arch in amd64 arm64; do
  stage="$WORK_DIR/$arch"
  mkdir -p "$stage"
  cp "$test_binary" "$stage/gitcrawl"
  cp "$ROOT/CHANGELOG.md" "$ROOT/LICENSE" "$ROOT/README.md" "$stage/"
  archive="gitcrawl_0.7.1_darwin_${arch}.tar.gz"
  tar -czf "$ARTIFACTS/$archive" -C "$stage" .
  shasum -a 256 "$ARTIFACTS/$archive" | awk -v name="$archive" '{ print $1 "  " name }' >> "$ARTIFACTS/checksums.txt"
done

: > "$MOCK_CODESIGN_LOG"
"$ROOT/scripts/verify-release.sh" v0.7.1 "$ARTIFACTS"
[[ "$(grep -F -c -- '--verify --strict --check-notarization -R=notarized' "$MOCK_CODESIGN_LOG")" == 2 ]]
if [[ ! -f "$MOCK_ARCH_LOG" ]] ||
  ! grep -E '^-x86_64 .*/amd64/gitcrawl --version$' "$MOCK_ARCH_LOG" >/dev/null ||
  ! grep -E '^-arm64 .*/arm64/gitcrawl --version$' "$MOCK_ARCH_LOG" >/dev/null; then
  echo "release verifier did not explicitly execute both architectures" >&2
  exit 1
fi
MOCK_ROSETTA_MISSING=1 "$ROOT/scripts/verify-release.sh" v0.7.1 "$ARTIFACTS" amd64
grep -Fx -- '--install-rosetta --agree-to-license' "$MOCK_ROSETTA_LOG" >/dev/null
[[ -f "$MOCK_ROSETTA_READY" ]]
if MOCK_MACOS_MINIMUM=14.0 "$ROOT/scripts/verify-release.sh" v0.7.1 "$ARTIFACTS" >/dev/null 2>&1; then
  echo "release verifier accepted a raised macOS deployment target" >&2
  exit 1
fi
if MOCK_NOTARIZATION_REJECT=1 \
  "$ROOT/scripts/verify-release.sh" v0.7.1 "$ARTIFACTS" >/dev/null 2>&1; then
  echo "release verifier accepted a missing notarization ticket" >&2
  exit 1
fi
if MOCK_CODESIGN_AUTHORITY='Developer ID Application: Peter Steinberger (Y5PE65HELJ)' \
  "$ROOT/scripts/verify-release.sh" v0.7.1 "$ARTIFACTS" >/dev/null 2>&1; then
  echo "personal signature was accepted" >&2
  exit 1
fi

echo unexpected > "$WORK_DIR/arm64/unexpected"
tar -czf "$ARTIFACTS/gitcrawl_0.7.1_darwin_arm64.tar.gz" \
  -C "$WORK_DIR/arm64" CHANGELOG.md LICENSE README.md gitcrawl unexpected
shasum -a 256 "$ARTIFACTS/gitcrawl_0.7.1_darwin_arm64.tar.gz" | \
  awk '{ print $1 "  gitcrawl_0.7.1_darwin_arm64.tar.gz" }' > "$ARTIFACTS/arm64.checksum"
awk '$2 != "gitcrawl_0.7.1_darwin_arm64.tar.gz"' "$ARTIFACTS/checksums.txt" > "$ARTIFACTS/checksums.next"
cat "$ARTIFACTS/arm64.checksum" >> "$ARTIFACTS/checksums.next"
mv "$ARTIFACTS/checksums.next" "$ARTIFACTS/checksums.txt"
if "$ROOT/scripts/verify-release.sh" v0.7.1 "$ARTIFACTS" >/dev/null 2>&1; then
  echo "archive with extra entries was accepted" >&2
  exit 1
fi

release_workflow="$ROOT/.github/workflows/release-unified.yml"
grep -F 'uses: openclaw/release-workflows/.github/workflows/release-go-cli.yml@v1' "$release_workflow" >/dev/null
grep -F 'checksum-filename: checksums.txt' "$release_workflow" >/dev/null
grep -F 'archive-files: '\''["CHANGELOG.md","LICENSE","README.md"]'\''' "$release_workflow" >/dev/null
grep -F 'stable-identifier: org.openclaw.gitcrawl' "$release_workflow" >/dev/null
grep -F 'darwin-universal: disabled' "$release_workflow" >/dev/null
grep -F 'require-signed-tag: true' "$release_workflow" >/dev/null
signers="$ROOT/.github/release-allowed-signers"
[[ -s "$signers" && ! -L "$signers" ]]
ssh-keygen -lf <(cut -d ' ' -f 2- "$signers") |
  grep -F 'SHA256:WmI9lVtd7F2c5XyRHbZVO3yYYJzwsSNzcZQMPT147HI' >/dev/null
if grep -R -F 'NOTARYTOOL_KEYCHAIN_PROFILE' "$ROOT/.github/workflows" >/dev/null; then
  echo "notary profile must not be configured in GitHub Actions" >&2
  exit 1
fi

echo "release script tests passed"
