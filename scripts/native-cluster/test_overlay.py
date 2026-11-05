#!/usr/bin/env python3
"""Run synthetic native cluster and code tests with their separate store overlays."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def run_overlay(root, native, store_sources, tag, packages, experiments=()):
    store_tests = sorted(native.glob("*_test.go"))
    sources = [*(native / name for name in store_sources), *store_tests]
    replacements = {
        str(root / "internal/store" / ("native_overlay_" + source.name)): str(source)
        for source in sources
    }
    tests = set()
    for source in [*store_tests, *sorted((native / "cmd").glob("*_test.go"))]:
        package = "internal/store" if source.parent == native else str(source.parent.relative_to(root))
        names = re.findall(r"(?m)^func (Test[A-Za-z0-9_]+)\(", source.read_text())
        tests.update(("github.com/openclaw/gitcrawl/" + package, name) for name in names if name not in experiments)
    if not tests:
        raise RuntimeError("no native overlay tests found")
    selector = "^(" + "|".join(sorted({name for _, name in tests})) + ")$"
    with tempfile.TemporaryDirectory(prefix="gitcrawl-native-overlay-") as scratch:
        overlay = Path(scratch) / "overlay.json"
        overlay.write_text(json.dumps({"Replace": replacements}))
        result = subprocess.run(
            [
                "go", "test", "-json", "-count=1", "-tags=" + tag,
                "-overlay=" + str(overlay), "-run=" + selector,
                *packages,
            ],
            cwd=root,
            env={**os.environ, "GOWORK": "off"},
            capture_output=True,
            text=True,
        )
    passed = set()
    completed = {}
    output = []
    for line in result.stdout.splitlines():
        event = json.loads(line)
        if event.get("Action") == "pass" and "Test" in event:
            passed.add((event["Package"], event["Test"]))
        if "Test" not in event and event.get("Action") in {"pass", "skip"}:
            completed[event["Package"]] = event["Action"]
        if "Output" in event:
            output.append(event["Output"])
    missing = tests - passed
    test_packages = {package for package, _ in tests}
    missing_packages = []
    for package in packages:
        name = "github.com/openclaw/gitcrawl/" + package[2:]
        # Go reports compile-only consumers with no test files as skipped.
        allowed = {"pass"} if name in test_packages else {"pass", "skip"}
        if completed.get(name) not in allowed:
            missing_packages.append(name)
    if result.returncode or missing or missing_packages:
        sys.stderr.write("".join(output) + result.stderr)
        if missing:
            print("native tests did not pass: " + ", ".join(name for _, name in sorted(missing)), file=sys.stderr)
        if missing_packages:
            print("native packages did not complete: " + ", ".join(missing_packages), file=sys.stderr)
        return result.returncode or 1
    print(f"{len(tests)} {native.name} overlay tests passed; all consumer packages compiled")
    return 0


def main():
    root = Path(__file__).resolve().parents[2]
    result = run_overlay(
        root, root / "scripts/native-cluster", ["native_bridge.go", "native_cluster.go"],
        "native_cluster_overlay",
        ["./internal/store", "./scripts/native-cluster/cmd",
         "./scripts/native-cluster/exportcmd", "./scripts/native-cluster/readercmd"],
        {"TestPreparedCPU", "TestNativeClusterMeasuredCommit", "TestNativeProducerMeasuredCommit"},
    )
    if result:
        return result
    return run_overlay(
        root, root / "scripts/native-code", ["native_store.go"], "native_code_overlay",
        ["./internal/store", "./scripts/native-code/cmd"],
    )


if __name__ == "__main__":
    raise SystemExit(main())
