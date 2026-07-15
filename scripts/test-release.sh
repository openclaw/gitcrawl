#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
EXPECTED_AUTHORITY='Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)'

for script in codesign-macos.sh package-release.sh verify-release.sh; do
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

cat > "$FAKE_BIN/lipo" <<'EOF'
#!/usr/bin/env bash
case "${2:-}" in
  */amd64/*) echo x86_64 ;;
  *) echo arm64 ;;
esac
EOF

chmod 0755 "$FAKE_BIN"/*
export PATH="$FAKE_BIN:$PATH"
export MOCK_CODESIGN_LOG="$WORK_DIR/codesign.log"

test_binary="$WORK_DIR/gitcrawl"
cat > "$test_binary" <<'EOF'
#!/usr/bin/env bash
[[ "${1:-}" == --version ]] || exit 2
echo 0.7.1
EOF
chmod 0755 "$test_binary"

GITCRAWL_REQUIRE_CODESIGN=0 "$ROOT/scripts/codesign-macos.sh" "$test_binary"
if CODESIGN_IDENTITY='Developer ID Application: Peter Steinberger (Y5PE65HELJ)' \
  "$ROOT/scripts/package-release.sh" v0.7.1 >/dev/null 2>&1; then
  echo "package script accepted personal signing identity" >&2
  exit 1
fi
if GITCRAWL_REQUIRE_CODESIGN=1 \
  CODESIGN_IDENTITY='Developer ID Application: Peter Steinberger (Y5PE65HELJ)' \
  "$ROOT/scripts/codesign-macos.sh" "$test_binary" >/dev/null 2>&1; then
  echo "personal signing identity was accepted" >&2
  exit 1
fi
GITCRAWL_REQUIRE_CODESIGN=1 \
  CODESIGN_IDENTITY="$EXPECTED_AUTHORITY" \
  "$ROOT/scripts/codesign-macos.sh" "$test_binary"

ARTIFACTS="$WORK_DIR/artifacts"
mkdir -p "$ARTIFACTS"
for arch in amd64 arm64; do
  stage="$WORK_DIR/$arch"
  mkdir -p "$stage"
  cp "$test_binary" "$stage/gitcrawl"
  cp "$ROOT/CHANGELOG.md" "$ROOT/LICENSE" "$ROOT/README.md" "$stage/"
  archive="gitcrawl_0.7.1_darwin_${arch}.tar.gz"
  tar -czf "$ARTIFACTS/$archive" -C "$stage" CHANGELOG.md LICENSE README.md gitcrawl
  shasum -a 256 "$ARTIFACTS/$archive" | awk -v name="$archive" '{ print $1 "  " name }' >> "$ARTIFACTS/checksums.txt"
done

"$ROOT/scripts/verify-release.sh" v0.7.1 "$ARTIFACTS"
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

grep -F -- '--identifier org.openclaw.gitcrawl' "$MOCK_CODESIGN_LOG" >/dev/null
grep -F 'FWJYW4S8P8' "$MOCK_CODESIGN_LOG" >/dev/null

release_workflow="$ROOT/.github/workflows/release-assets.yml"
grep -F 'contents: write' "$release_workflow" >/dev/null
grep -F "github.ref == format('refs/heads/{0}', github.event.repository.default_branch)" "$release_workflow" >/dev/null
grep -F "endsWith(github.workflow_ref, format('@refs/heads/{0}', github.event.repository.default_branch))" "$release_workflow" >/dev/null
grep -F "github.event_name == 'release' && github.event.action == 'published'" "$release_workflow" >/dev/null
# shellcheck disable=SC2016 # GitHub expression must remain literal.
grep -F 'ref: ${{ github.event.repository.default_branch }}' "$release_workflow" >/dev/null
grep -F 'persist-credentials: false' "$release_workflow" >/dev/null
# shellcheck disable=SC2016 # GitHub expression must remain literal.
[[ "$(grep -F -c 'GH_TOKEN: ${{ github.token }}' "$release_workflow")" == 1 ]]
# shellcheck disable=SC2016 # jq expression must remain literal.
grep -F 'tag_name == $tag and (.draft == ($draft == "true"))' "$release_workflow" >/dev/null
grep -F 'Accept: application/octet-stream' "$release_workflow" >/dev/null
grep -F 'unset GH_TOKEN GITHUB_TOKEN' "$release_workflow" >/dev/null
if grep -F 'gh release download' "$release_workflow" >/dev/null; then
  echo "release workflow cannot resolve draft assets through gh release download" >&2
  exit 1
fi

echo "release script tests passed"
