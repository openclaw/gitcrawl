#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VERSION=${1:-}
EXPECTED_AUTHORITY='Developer ID Application: OpenClaw Foundation (FWJYW4S8P8)'

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  echo "usage: $0 vX.Y.Z" >&2
  exit 2
fi
[[ "$(uname -s)" == Darwin ]] || {
  echo "official release packaging must run on macOS" >&2
  exit 1
}
[[ "$(uname -m)" == arm64 ]] || {
  echo "official release packaging requires Apple Silicon with Rosetta for both architecture smoke tests" >&2
  exit 1
}
[[ "${CODESIGN_IDENTITY:-}" == "$EXPECTED_AUTHORITY" ]] || {
  echo "official releases require $EXPECTED_AUTHORITY" >&2
  exit 1
}
[[ -n "${NOTARYTOOL_KEYCHAIN_PROFILE:-}" ]] || {
  echo "official releases require NOTARYTOOL_KEYCHAIN_PROFILE at runtime" >&2
  exit 1
}

for tool in codesign ditto git go goreleaser lipo plutil shasum tar xcrun; do
  command -v "$tool" >/dev/null || {
    echo "missing required tool: $tool" >&2
    exit 1
  }
done

head_commit=$(git -C "$ROOT" rev-parse HEAD)
tag_commit=$(git -C "$ROOT" rev-parse "refs/tags/$VERSION^{commit}" 2>/dev/null) || {
  echo "release tag does not exist locally: $VERSION" >&2
  exit 1
}
[[ "$head_commit" == "$tag_commit" ]] || {
  echo "HEAD does not match release tag $VERSION" >&2
  exit 1
}
[[ -z "$(git -C "$ROOT" status --porcelain --untracked-files=normal)" ]] || {
  echo "release checkout is not clean" >&2
  exit 1
}
git -C "$ROOT" tag -v "$VERSION" >/dev/null 2>&1 || {
  echo "release tag is not signed by a trusted git signing key: $VERSION" >&2
  exit 1
}

release_version=${VERSION#v}
for arch in amd64 arm64; do
  archive="$ROOT/dist/gitcrawl_${release_version}_darwin_${arch}.tar.gz"
  [[ ! -e "$archive" ]] || {
    echo "refusing to overwrite existing artifact: $archive" >&2
    exit 1
  }
done

(
  cd "$ROOT"
  CODESIGN_IDENTITY="$EXPECTED_AUTHORITY" \
    GITCRAWL_REQUIRE_CODESIGN=1 \
    GOWORK=off \
    goreleaser release --clean --skip=publish
)

"$ROOT/scripts/verify-release.sh" "$VERSION"
