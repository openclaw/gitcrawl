import copy
from decimal import Decimal
import gzip
from pathlib import Path
import tempfile
import unittest

import cluster_simd_parity as parity


class ComparatorTests(unittest.TestCase):
    def fixture(self):
        # Small snapshots isolate comparison rules; the Go producer checks the
        # full 2000-node fixture and all four phases during the end-to-end run.
        return {"phases": [{"output": {"vector_coverage": {"processed": 2}},
                            "vectors": [{"thread_id": 1, "vector_sha256": "a" * 64}],
                            "snapshot": {
                                "cluster_groups": [{"id": 1, "representative_thread_id": 1,
                                                    "created_at": "2026-01-01T00:00:00Z",
                                                    "updated_at": "2026-01-01T00:00:01Z", "closed_at": None}],
                                "cluster_memberships": [{"cluster_id": 1, "thread_id": 1,
                                                         "score_to_representative": Decimal("0.5"),
                                                         "created_at": "2026-01-01T00:00:00Z",
                                                         "updated_at": "2026-01-01T00:00:01Z", "removed_at": None}],
                                "cluster_runs": [{"started_at": "2026-01-01T00:00:00Z",
                                                  "finished_at": "2026-01-01T00:00:01Z"}],
                                "cluster_events": [{"created_at": "2026-01-01T00:00:00Z"}],
                            }}]}

    def check_pair(self, left, right):
        return parity.compare(parity.normalized(left), parity.normalized(right))

    def test_accepts_generated_clock_shift_and_only_small_score_delta(self):
        left = self.fixture()
        right = copy.deepcopy(left)
        right["phases"][0]["snapshot"]["cluster_groups"][0]["created_at"] = "2025-12-31T23:59:59Z"
        right["phases"][0]["snapshot"]["cluster_memberships"][0]["score_to_representative"] += Decimal("1e-12")
        self.assertEqual(self.check_pair(left, right), Decimal("1e-12"))
        self.assertEqual(left["phases"][0]["snapshot"]["cluster_groups"][0]["created_at"], "2026-01-01T00:00:00Z")

    def test_rejects_semantic_changes(self):
        mutations = [
            ("topology", lambda p: p["snapshot"]["cluster_memberships"][0].update(cluster_id=2)),
            ("representative", lambda p: p["snapshot"]["cluster_groups"][0].update(representative_thread_id=2)),
            ("coverage", lambda p: p["output"]["vector_coverage"].update(processed=3)),
            ("vector identity", lambda p: p["vectors"][0].update(vector_sha256="b" * 64)),
            ("non-score number", lambda p: p["snapshot"]["cluster_groups"][0].update(id=Decimal("1.0000000000001"))),
            ("score null", lambda p: p["snapshot"]["cluster_memberships"][0].update(score_to_representative=None)),
            ("score delta", lambda p: p["snapshot"]["cluster_memberships"][0].update(score_to_representative=Decimal("0.5000000000011"))),
            ("score NaN", lambda p: p["snapshot"]["cluster_memberships"][0].update(score_to_representative=Decimal("NaN"))),
            ("row count", lambda p: p["vectors"].clear()),
            ("clock presence", lambda p: p["snapshot"]["cluster_groups"][0].update(closed_at="2026-01-01T00:00:01Z")),
            ("clock order", lambda p: p["snapshot"]["cluster_groups"][0].update(updated_at="2025-12-31T23:59:59Z")),
            ("missing clock", lambda p: p["snapshot"]["cluster_groups"][0].pop("created_at")),
        ]
        for name, mutate in mutations:
            with self.subTest(name=name):
                left = self.fixture()
                right = copy.deepcopy(left)
                mutate(right["phases"][0])
                with self.assertRaises(ValueError):
                    self.check_pair(left, right)

    def test_score_tolerance_does_not_apply_to_other_paths(self):
        with self.assertRaises(ValueError):
            parity.compare({"score_to_representative": Decimal("0.5")},
                           {"score_to_representative": Decimal("0.5000000000001")})

    def test_requires_complete_phase_order(self):
        value = {"schema": 1, "status": "pass", "fixture": "clusterBenchmarkData(2000);PCG(7,11)",
                 "thread_input_sha256": "a" * 64, "phases": []}
        with self.assertRaisesRegex(ValueError, "missing/reordered phases"):
            parity.validate_artifact(value)

    def test_rejects_nanosecond_clock_reversal(self):
        value = self.fixture()
        value["phases"][0]["snapshot"]["cluster_groups"][0].update(
            created_at="2026-01-01T00:00:00.000000002Z",
            updated_at="2026-01-01T00:00:00.000000001Z")
        with self.assertRaisesRegex(ValueError, "clock order invalid"):
            parity.normalized(value)

    def test_clock_offsets_and_nanoseconds_remain_exact(self):
        self.assertEqual(parity.clock_nanoseconds("2026-01-01T01:00:00.000000001+01:00"),
                         parity.clock_nanoseconds("2026-01-01T00:00:00.000000001Z"))
        self.assertEqual(parity.clock_nanoseconds("2026-01-01T00:00:00.000000002Z") -
                         parity.clock_nanoseconds("2026-01-01T00:00:00.000000001Z"), 1)

    def test_stored_json_remains_lossless(self):
        left = self.fixture()
        left["phases"][0]["snapshot"]["cluster_groups"][0]["params_json"] = {"stored_json": '{"x":1}'}
        for raw in ('{"x":1,"x":2}', '{"x":1} trailing', 'null'):
            with self.subTest(raw=raw):
                right = copy.deepcopy(left)
                right["phases"][0]["snapshot"]["cluster_groups"][0]["params_json"] = {"stored_json": raw}
                with self.assertRaises(ValueError):
                    self.check_pair(left, right)
        right = copy.deepcopy(left)
        left["phases"][0]["snapshot"]["cluster_groups"][0]["params_json"] = None
        right["phases"][0]["snapshot"]["cluster_groups"][0]["params_json"] = {"stored_json": "null"}
        with self.assertRaises(ValueError):
            self.check_pair(left, right)

    def test_rejects_ambiguous_or_nonfinite_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "arm.json.gz"
            for text in ('{"schema":1,"schema":1}', '{"value":NaN}', '{"value":Infinity}'):
                with self.subTest(text=text):
                    with gzip.open(path, "wb") as stream:
                        stream.write(text.encode())
                    with self.assertRaises(ValueError):
                        parity.load_artifact(path)


if __name__ == "__main__":
    unittest.main()
