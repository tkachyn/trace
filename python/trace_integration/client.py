"""subprocess client for the versioned Trace command line boundary"""

from __future__ import annotations

import json
import subprocess
from typing import Any

from .proposal import ExtractionProposal


class TraceClientError(RuntimeError):
    """report a failed Trace command line operation"""

    def __init__(self, message: str, result: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.result = result or {}


class TraceClient:
    """submit proposals to the Go Trace core without opening storage directly"""

    def __init__(
        self,
        trace_executable: str = "trace",
        runner: Any = subprocess.run,
    ) -> None:
        self.trace_executable = trace_executable
        self._runner = runner

    def ingest(
        self,
        trace_path: str,
        proposal: ExtractionProposal | dict[str, Any],
        *,
        jsonl: bool = False,
    ) -> dict[str, Any]:
        if isinstance(proposal, ExtractionProposal):
            payload = proposal.to_batch_request()
        else:
            payload = proposal
        if jsonl:
            encoded = self._encode_jsonl(payload)
            command = [self.trace_executable, "ingest", "--jsonl", trace_path]
        else:
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            command = [self.trace_executable, "ingest", trace_path]
        completed = self._runner(
            command,
            input=encoded,
            text=True,
            capture_output=True,
            check=False,
        )
        result: dict[str, Any] = {}
        if completed.stdout:
            try:
                result = json.loads(completed.stdout)
            except json.JSONDecodeError as error:
                raise TraceClientError(
                    f"Trace returned invalid JSON: {error}",
                ) from error
        if completed.returncode != 0:
            message = completed.stderr.strip() or "Trace ingestion failed"
            raise TraceClientError(message, result)
        if result.get("errors"):
            raise TraceClientError("Trace rejected the ingestion request", result)
        return result

    def ingest_proposal(
        self,
        trace_path: str,
        proposal: ExtractionProposal,
        *,
        jsonl: bool = False,
    ) -> dict[str, Any]:
        return self.ingest(trace_path, proposal, jsonl=jsonl)

    @staticmethod
    def _encode_jsonl(request: dict[str, Any]) -> str:
        lines: list[dict[str, Any]] = [
            {
                "type": "header",
                "protocol": request["protocol"],
                "version": request["version"],
                "request_id": request["request_id"],
                "actor": request.get("actor", ""),
            }
        ]
        for key, line_type in (
            ("events", "event"),
            ("memories", "memory"),
            ("entities", "entity"),
            ("relations", "relation"),
        ):
            for record in request.get(key, []):
                lines.append({"type": line_type, **record})
        lines.append({"type": "commit"})
        return "\n".join(json.dumps(line, sort_keys=True) for line in lines) + "\n"
