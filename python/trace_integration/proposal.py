"""provider-neutral extraction proposals for the Trace ingestion boundary"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any


PROTOCOL = "trace.extraction_proposal"
VERSION = 1


@dataclass
class EventProposal:
    client_id: str
    event_type: str
    payload: Any
    namespace: str = ""
    recorded_at: str = ""
    occurred_at: str = ""
    actor: str = ""
    provenance: dict[str, Any] = field(default_factory=dict)
    extensions: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "client_id": self.client_id,
            "event_type": self.event_type,
            "payload": self.payload,
            "provenance": self.provenance,
        }
        for key in ("namespace", "recorded_at", "occurred_at", "actor"):
            current = getattr(self, key)
            if current:
                value[key] = current
        if self.extensions:
            value["extensions"] = self.extensions
        return value


@dataclass
class MemoryProposal:
    client_id: str
    kind: str
    content: str = ""
    structured_value: Any = None
    namespace: str = ""
    recorded_at: str = ""
    valid_from: str = ""
    valid_to: str = ""
    predicate: str = ""
    object_value: str = ""
    confidence: Any = None
    provenance: dict[str, Any] = field(default_factory=dict)
    derived_from: list[str] = field(default_factory=list)
    extensions: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "client_id": self.client_id,
            "kind": self.kind,
            "provenance": self.provenance,
        }
        for key in (
            "content",
            "namespace",
            "recorded_at",
            "valid_from",
            "valid_to",
            "predicate",
            "object_value",
        ):
            current = getattr(self, key)
            if current:
                value[key] = current
        if self.structured_value is not None:
            value["structured_value"] = self.structured_value
        if self.confidence is not None:
            value["confidence"] = self.confidence
        if self.derived_from:
            value["derived_from"] = self.derived_from
        if self.extensions:
            value["extensions"] = self.extensions
        return value


@dataclass
class ExtractionProposal:
    proposal_id: str
    namespace: str
    actor: str
    events: list[EventProposal] = field(default_factory=list)
    memories: list[MemoryProposal] = field(default_factory=list)
    unmapped: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    version: int = VERSION

    def validate(self) -> None:
        if self.version != VERSION:
            raise ValueError(f"unsupported extraction proposal version {self.version}")
        if not self.proposal_id:
            raise ValueError("proposal_id is required")
        if not self.namespace:
            raise ValueError("namespace is required")
        client_ids = [event.client_id for event in self.events]
        client_ids.extend(memory.client_id for memory in self.memories)
        if any(not client_id for client_id in client_ids):
            raise ValueError("every proposal record requires client_id")
        if len(client_ids) != len(set(client_ids)):
            raise ValueError("proposal client_id values must be unique")
        event_ids = {event.client_id for event in self.events}
        for memory in self.memories:
            for source_id in memory.derived_from:
                if source_id not in event_ids:
                    raise ValueError(
                        f"memory {memory.client_id} references unknown proposal source {source_id}"
                    )

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "type": PROTOCOL,
            "version": self.version,
            "proposal_id": self.proposal_id,
            "namespace": self.namespace,
            "actor": self.actor,
            "events": [event.to_dict() for event in self.events],
            "memories": [memory.to_dict() for memory in self.memories],
            "unmapped": self.unmapped,
            "warnings": self.warnings,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ExtractionProposal":
        events = [EventProposal(**event) for event in value.get("events", [])]
        memories = [MemoryProposal(**memory) for memory in value.get("memories", [])]
        proposal = cls(
            proposal_id=value.get("proposal_id", ""),
            namespace=value.get("namespace", ""),
            actor=value.get("actor", ""),
            events=events,
            memories=memories,
            unmapped=value.get("unmapped", []),
            warnings=value.get("warnings", []),
            version=value.get("version", 0),
        )
        proposal.validate()
        return proposal

    @classmethod
    def from_json(cls, value: str) -> "ExtractionProposal":
        return cls.from_dict(json.loads(value))

    def to_batch_request(self) -> dict[str, Any]:
        self.validate()
        events = []
        for event in self.events:
            value = event.to_dict()
            value["namespace"] = value.get("namespace", self.namespace)
            events.append(value)
        memories = []
        for memory in self.memories:
            value = memory.to_dict()
            value["namespace"] = value.get("namespace", self.namespace)
            memories.append(value)
        return {
            "protocol": "trace.batch",
            "version": VERSION,
            "request_id": self.proposal_id,
            "actor": self.actor,
            "events": events,
            "memories": memories,
        }
