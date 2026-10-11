#!/usr/bin/env bash
set -euo pipefail

binary=${1:?usage: check-macos-minimum.sh binary [snapshot]}
if [[ "$(uname -s)" != Darwin ]]; then
  # Linux CI snapshots cross-compile Darwin; official releases use macOS.
  [[ "${2:-false}" == true ]] && exit 0
  echo "macOS deployment-target verification requires macOS" >&2
  exit 1
fi

architectures=$(lipo -archs "$binary")
[[ -n "$architectures" ]] || exit 1
for architecture in $architectures; do
  load_commands=$(otool -arch "$architecture" -l "$binary")
  printf '%s\n' "$load_commands" | grep -A3 LC_BUILD_VERSION
  minimum=$(awk '/cmd LC_BUILD_VERSION/ { build = 1; next } build && $1 == "minos" { print $2; build = 0 }' <<<"$load_commands")
  if ! awk -v minimum="$minimum" 'BEGIN {
    if (minimum !~ /^[0-9]+\.[0-9]+(\.[0-9]+)?$/) exit 1
    split(minimum, v, ".")
    exit !(v[1] < 13 || (v[1] == 13 && v[2] == 0 && v[3] + 0 == 0))
  }'; then
    echo "unsupported macOS deployment target for $architecture: ${minimum:-missing} (maximum 13.0)" >&2
    exit 1
  fi
done
