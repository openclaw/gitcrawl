#!/usr/bin/env python3
"""Compare synthetic durable cluster output across scalar/SIMD/fallback builds."""

import copy
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PHASES = ["full", "limited", "model_partial", "strict_rejected"]
CLOCKS = {
    "cluster_groups": {"created_at", "updated_at", "closed_at"},
    "cluster_memberships": {"created_at", "updated_at", "removed_at"},
    "cluster_runs": {"started_at", "finished_at"},
    "cluster_events": {"created_at"},
}
NULLABLE_CLOCKS = {"closed_at", "removed_at", "finished_at"}
TIMESTAMP = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)\Z")
HEX = re.compile(r"[0-9a-f]{64}\Z")
TOLERANCE = Decimal("1e-12")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def reject_constant(value):
    raise ValueError("non-finite JSON value: " + value)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def load_artifact(path):
    require(path.stat().st_size <= 2 << 20, "compressed arm exceeds 2 MiB")
    with gzip.open(path, "rb") as stream:
        raw = stream.read((32 << 20) + 1)
    require(len(raw) <= 32 << 20, "decoded arm exceeds 32 MiB")
    value = json.loads(raw, parse_float=Decimal, parse_constant=reject_constant,
                       object_pairs_hook=unique_object)
    validate_artifact(value)
    return value


def validate_artifact(value):
    require(set(value) == {"schema", "fixture", "thread_input_sha256", "status", "phases"}, "unexpected artifact fields")
    require(value["schema"] == 1 and value["status"] == "pass", "oracle did not complete")
    require(value["fixture"] == "clusterBenchmarkData(2000);PCG(7,11)", "unexpected fixture")
    require(bool(HEX.fullmatch(value["thread_input_sha256"])), "invalid input digest")
    phases = value["phases"]
    require([p["name"] for p in phases] == PHASES, "missing/reordered phases")
    for index, phase in enumerate(phases):
        model = "text-embedding-3-small" if index < 2 else "text-embedding-3-large"
        count = 2000 if index == 0 else 1997
        require(phase["selected_thread_ids"] == list(range(1, count + 1)), "selected IDs differ")
        require(phase["config"] == {"model": model, "basis": "title_original", "dimensions": 1024}, "vector configuration differs")
        require(set(phase["snapshot"]) == set(CLOCKS), "unexpected durable tables")
        vectors = phase["vectors"]
        identities = {(v["thread_id"], v["basis"], v["model"]) for v in vectors}
        expected = {(i, "title_original", "text-embedding-3-small") for i in range(1, 2001)}
        if index >= 2:
            expected |= {(i, "title_original", "text-embedding-3-large") for i in range(1, 1998)}
        require(identities == expected and len(vectors) == len(expected), "vector identities differ")
        for vector in vectors:
            require(vector["dimensions"] == 1024 and vector["backend"] == "exact", "vector metadata differs")
            require(bool(vector["content_hash"]) and bool(HEX.fullmatch(vector["vector_sha256"])), "vector digest missing")
        if index < 3:
            covered = 2000 if index < 2 else 1997
            expected_coverage = {"supported": True, "fallback": False, "eligible": 2000,
                                 "covered": covered, "fresh": covered, "processed": count,
                                 "missing": 2000 - covered, "missing_input": 0, "stale": 0,
                                 "partial": index > 0, "complete": index == 0}
            require(phase["output"]["vector_coverage"] == expected_coverage, "coverage differs")
            require(phase["output"]["vector_count"] == count and phase["output"]["run_id"] > 0, "run/vector count differs")
            require(len(phase["snapshot"]["cluster_runs"]) == index + 1, "persisted run count differs")
        else:
            require("vector coverage is incomplete" in phase["error"], "strict error missing")
            require(phase["snapshot"] == phases[2]["snapshot"], "strict rejection mutated persistence")
            require(phase["vectors"] == phases[2]["vectors"], "strict rejection mutated vectors")


def clock_nanoseconds(value):
    require(isinstance(value, str), "clock must be text")
    match = TIMESTAMP.fullmatch(value)
    require(match is not None, "invalid clock")
    whole = datetime.fromisoformat(match[1] + match[3].replace("Z", "+00:00"))
    delta = whole.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    seconds = delta.days * 86400 + delta.seconds
    return seconds * 10**9 + int((match[2] or "0").ljust(9, "0"))


def normalized(artifact):
    result = copy.deepcopy(artifact)
    for phase in result["phases"]:
        for table, clocks in CLOCKS.items():
            for row in phase["snapshot"][table]:
                require(clocks <= row.keys(), table + " clock columns missing")
                parsed = {}
                for column in clocks:
                    value = row[column]
                    if value is None:
                        require(column in NULLABLE_CLOCKS, table + "." + column + " cannot be null")
                        continue
                    parsed[column] = clock_nanoseconds(value)
                    row[column] = {"generated_clock": "present"}
                for start, end in (("created_at", "updated_at"), ("started_at", "finished_at"),
                                   ("created_at", "removed_at"), ("created_at", "closed_at")):
                    if start in parsed and end in parsed:
                        require(parsed[start] <= parsed[end], table + " clock order invalid")
    return result


def compare(left, right, path=(), maximum=Decimal(0)):
    score = (len(path) == 6 and path[0] == "phases" and path[2:4] == ("snapshot", "cluster_memberships")
             and path[-1] == "score_to_representative")
    if score and left is not None and right is not None:
        require(type(left) in (int, Decimal) and type(right) in (int, Decimal), "invalid score at " + str(path))
        a, b = Decimal(left), Decimal(right)
        require(a.is_finite() and b.is_finite(), "non-finite score at " + str(path))
        delta = abs(a - b)
        require(delta <= TOLERANCE, "score delta exceeds 1e-12 at " + str(path))
        return max(maximum, delta)
    require(type(left) is type(right), "type/null mismatch at " + str(path))
    if isinstance(left, dict):
        require(left.keys() == right.keys(), "field mismatch at " + str(path))
        for key in sorted(left):
            maximum = compare(left[key], right[key], path + (key,), maximum)
    elif isinstance(left, list):
        require(len(left) == len(right), "row count mismatch at " + str(path))
        for i, (a, b) in enumerate(zip(left, right)):
            maximum = compare(a, b, path + (i,), maximum)
    else:
        require(left == right, "exact-value mismatch at " + str(path))
    return maximum


def execute(label, command, env, timeout):
    # A private process group bounds only this invocation and its own children.
    with subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, start_new_session=True) as child:
        try:
            output, _ = child.communicate(timeout=timeout)
        except BaseException:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.communicate()
            raise
        if child.returncode:
            raise RuntimeError(label + " failed:\n" + output)
    return output


def main():
    require(os.name == "posix", "runner supports Linux and macOS")
    env = {k: v for k, v in os.environ.items() if not k.startswith("GITCRAWL_")}
    env.update(GOENV="off", GOWORK="off", GOTOOLCHAIN="local", GOFLAGS="", GOEXPERIMENT="", GODEBUG="")
    version = execute("Go version", ["go", "version"], env, 30).strip()
    with tempfile.TemporaryDirectory(prefix="gitcrawl-simd-parity-") as temporary:
        directory = Path(temporary)
        binaries, hashes = {}, {}
        for name, experiment in (("scalar", ""), ("simd", "simd")):
            binary = directory / (name + ".test")
            execute(name + " build", ["go", "test", "-mod=readonly", "-c", "-o", str(binary), "./internal/cli"],
                    {**env, "GOEXPERIMENT": experiment}, 240)
            binaries[name] = binary
            hashes[name] = hashlib.sha256(binary.read_bytes()).hexdigest()
        require(hashes["scalar"] != hashes["simd"], "scalar and SIMD-enabled binaries are identical")
        artifacts = {}
        for name, build, debug in (("scalar", "scalar", ""), ("simd", "simd", ""), ("fallback", "simd", "simd=0")):
            path = directory / (name + ".json.gz")
            output = execute(name + " oracle", [str(binaries[build]), "-test.run", "^TestClusterCommand2000DurableOracle$",
                             "-test.v", "-test.count=1", "-test.timeout=2m"],
                             {**env, "GODEBUG": debug, "GITCRAWL_DURABLE_ORACLE_OUTPUT": str(path)}, 150)
            require("--- PASS: TestClusterCommand2000DurableOracle " in output and "--- SKIP:" not in output,
                    name + " oracle did not execute")
            artifacts[name] = normalized(load_artifact(path))
        simd_delta = compare(artifacts["scalar"], artifacts["simd"])
        fallback_delta = compare(artifacts["scalar"], artifacts["fallback"])
        print(json.dumps({"status": "pass", "go": version, "fixture": artifacts["scalar"]["fixture"],
                          "phases": PHASES, "binary_sha256": hashes,
                          "simd_enabled_max_score_delta": str(simd_delta),
                          "forced_fallback_max_score_delta": str(fallback_delta),
                          "score_absolute_tolerance": str(TOLERANCE)}, sort_keys=True))


if __name__ == "__main__":
    main()
