"""dependency-free Python client for the versioned Trace HTTP API"""

from __future__ import annotations

import json
from typing import Any
from urllib import error, parse, request

from .proposal import ExtractionProposal


class TraceHTTPError(RuntimeError):
    """report a stable Trace HTTP API error"""

    def __init__(
        self,
        message: str,
        *,
        status: int,
        category: str,
        request_id: str = "",
        payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.category = category
        self.request_id = request_id
        self.payload = payload or {}


class HTTPTraceClient:
    """call the local Trace API without opening the file directly"""

    def __init__(
        self,
        base_url: str,
        *,
        principal: str | None = None,
        purpose: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.principal = principal
        self.purpose = purpose
        self.timeout = timeout

    def ingest(self, proposal: ExtractionProposal | dict[str, Any]) -> dict[str, Any]:
        if isinstance(proposal, ExtractionProposal):
            proposal = proposal.to_batch_request()
        return self._request("POST", "/v1/ingest", proposal)

    def inspect(self) -> dict[str, Any]:
        return self._request("GET", "/v1/inspect")

    def validate(self) -> dict[str, Any]:
        return self._request("POST", "/v1/validate", {})

    def query(self, **filters: Any) -> dict[str, Any]:
        return self._request("POST", "/v1/query", self._with_access(filters))

    def search(self, **filters: Any) -> dict[str, Any]:
        return self._request("POST", "/v1/search", self._with_access(filters))

    def explain(self, target_id: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "/v1/explain",
            self._with_access({"target_id": target_id}),
        )

    def history(self, target_id: str = "") -> list[dict[str, Any]]:
        return self._request(
            "POST",
            "/v1/history",
            self._with_access({"target_id": target_id}),
        )

    def forget(
        self,
        target_id: str,
        *,
        mode: str = "dependency_closure",
        actor: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._request(
            "POST",
            "/v1/forget",
            {
                "target_id": target_id,
                "mode": mode,
                "actor": actor or self.principal or "",
                "purpose": self.purpose or "",
            },
        )

    def get_event(self, record_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/events/{parse.quote(record_id)}", query=True)

    def get_memory(self, record_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/memories/{parse.quote(record_id)}", query=True)

    def _with_access(self, value: dict[str, Any]) -> dict[str, Any]:
        payload = dict(value)
        payload["principal"] = self.principal or ""
        payload["purpose"] = self.purpose or ""
        return payload

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        query: bool = False,
    ) -> Any:
        url = self.base_url + path
        if query:
            params = {
                "principal": self.principal or "",
                "purpose": self.purpose or "",
            }
            url += "?" + parse.urlencode(params)
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            headers["Content-Type"] = "application/json"
        http_request = request.Request(url, data=body, headers=headers, method=method)
        try:
            with request.urlopen(http_request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as failure:
            raw = failure.read().decode("utf-8")
            try:
                value = json.loads(raw)
            except json.JSONDecodeError:
                value = {}
            detail = value.get("error", {})
            raise TraceHTTPError(
                detail.get("message", failure.reason),
                status=failure.code,
                category=detail.get("category", "storage"),
                request_id=detail.get("request_id", ""),
                payload=value,
            ) from failure
