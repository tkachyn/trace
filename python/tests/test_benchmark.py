import unittest

from trace_integration.benchmark.results import (
    BenchmarkCaseResult,
    BenchmarkReport,
)


class BenchmarkTests(unittest.TestCase):
    def test_report_contains_required_summary_fields(self):
        report = BenchmarkReport(
            metadata={
                "trace_schema_version": 2,
                "dataset_hash": "dataset-hash",
                "seed": 7,
            },
            cases=[
                BenchmarkCaseResult(
                    case_id="case-1",
                    extraction_seconds=0.1,
                    ingestion_seconds=0.2,
                ),
                BenchmarkCaseResult(
                    case_id="case-2",
                    error="rejected",
                    abstained=True,
                ),
            ],
        )

        value = report.to_dict()

        self.assertEqual(value["summary"]["cases"], 2)
        self.assertEqual(value["summary"]["errors"], 1)
        self.assertEqual(value["summary"]["abstentions"], 1)
        self.assertIn("p95_seconds", value["summary"])
        self.assertEqual(value["metadata"]["dataset_hash"], "dataset-hash")


if __name__ == "__main__":
    unittest.main()
