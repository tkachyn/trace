"""small deterministic benchmark helpers for Trace integrations"""

from .results import BenchmarkCaseResult, BenchmarkReport
from .runner import run

__all__ = ["BenchmarkCaseResult", "BenchmarkReport", "run"]
