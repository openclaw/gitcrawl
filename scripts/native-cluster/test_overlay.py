#!/usr/bin/env python3
"""Run the synthetic native producer and graph tests with their store overlay."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def main():
    root = Path(__file__).resolve().parents[2]
    native = root / "scripts/native-cluster"
    store_tests = sorted(native.glob("*_test.go"))
    sources = [native / "native_bridge.go", native / "native_cluster.go", *store_tests]
    replacements = {
        str(root / "internal/store" / ("native_overlay_" + source.name)): str(source)
        for source in sources
    }
    tests = set()
    experiments = {
        "TestPreparedCPU", "TestNativeClusterMeasuredCommit", "TestNativeProducerMeasuredCommit",
    }
    for source in [*store_tests, *sorted((native / "cmd").glob("*_test.go"))]:
        package = "internal/store" if source.parent == native else "scripts/native-cluster/cmd"
        names = re.findall(r"(?m)^func (Test[A-Za-z0-9_]+)\(", source.read_text())
        tests.update(("github.com/openclaw/gitcrawl/" + package, name) for name in names if name not in experiments)
    if not tests:
        raise RuntimeError("no native overlay tests found")
    selector = "^(" + "|".join(sorted({name for _, name in tests})) + ")$"
    packages = [
        "./internal/store", "./scripts/native-cluster/cmd",
        "./scripts/native-cluster/exportcmd", "./scripts/native-cluster/readercmd",
    ]
    with tempfile.TemporaryDirectory(prefix="gitcrawl-native-overlay-") as scratch:
        overlay = Path(scratch) / "overlay.json"
        overlay.write_text(json.dumps({"Replace": replacements}))
        result = subprocess.run(
            [
                "go", "test", "-json", "-count=1", "-tags=native_cluster_overlay",
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
    print(f"{len(tests)} native overlay tests passed; exporter and reader compiled")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
