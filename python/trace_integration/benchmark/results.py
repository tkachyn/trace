"""portable benchmark result structures and deterministic summaries"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any


@dataclass
class BenchmarkCaseResult:
    case_id: str
    extraction_seconds: float = 0.0
    ingestion_seconds: float = 0.0
    error: str = ""
    abstained: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "extraction_seconds": self.extraction_seconds,
            "ingestion_seconds": self.ingestion_seconds,
            "error": self.error,
            "abstained": self.abstained,
        }


@dataclass
class BenchmarkReport:
    metadata: dict[str, Any]
    cases: list[BenchmarkCaseResult] = field(default_factory=list)
    runner_version: str = "trace-python-integration-0.1"

    def summary(self) -> dict[str, Any]:
        durations = sorted(
            case.extraction_seconds + case.ingestion_seconds for case in self.cases
        )
        return {
            "cases": len(self.cases),
            "errors": sum(bool(case.error) for case in self.cases),
            "abstentions": sum(case.abstained for case in self.cases),
            "p50_seconds": _percentile(durations, 0.50),
            "p95_seconds": _percentile(durations, 0.95),
            "p99_seconds": _percentile(durations, 0.99),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "runner_version": self.runner_version,
            "metadata": self.metadata,
            "summary": self.summary(),
            "cases": [case.to_dict() for case in self.cases],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    index = min(len(values) - 1, int(round((len(values) - 1) * percentile)))
    return values[index]
