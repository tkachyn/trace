"""benchmark extraction and Go-core ingestion without provider assumptions"""

from __future__ import annotations

from collections.abc import Iterable
import time
from typing import Any, Protocol

from ..client import TraceClient
from ..proposal import ExtractionProposal
from .results import BenchmarkCaseResult, BenchmarkReport


class Extractor(Protocol):
    """produce one extraction proposal from a benchmark case"""

    def __call__(self, case: dict[str, Any]) -> ExtractionProposal:
        ...


def run(
    cases: Iterable[dict[str, Any]],
    extractor: Extractor,
    client: TraceClient,
    trace_path: str,
    *,
    metadata: dict[str, Any] | None = None,
) -> BenchmarkReport:
    """run a provider-neutral extraction and ingestion benchmark"""
    report = BenchmarkReport(metadata=metadata or {})
    for index, case in enumerate(cases):
        case_id = str(case.get("id", index))
        result = BenchmarkCaseResult(case_id=case_id)
        try:
            started = time.perf_counter()
            proposal = extractor(case)
            result.extraction_seconds = time.perf_counter() - started
            started = time.perf_counter()
            client.ingest_proposal(trace_path, proposal)
            result.ingestion_seconds = time.perf_counter() - started
        except Exception as error:  # noqa: broad-exception-caught
            result.error = str(error)
        report.cases.append(result)
    return report
