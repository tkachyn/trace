import unittest

from trace_integration.proposal import (
    EventProposal,
    ExtractionProposal,
    MemoryProposal,
)


class ProposalTests(unittest.TestCase):
    def test_round_trip_preserves_provider_extensions(self):
        proposal = ExtractionProposal(
            proposal_id="proposal-1",
            namespace="user:tim",
            actor="agent:extractor",
            events=[
                EventProposal(
                    client_id="event-1",
                    event_type="conversation.message",
                    payload={"content": "I prefer Go"},
                    provenance={"provider": "example", "model": "test-model"},
                )
            ],
            memories=[
                MemoryProposal(
                    client_id="memory-1",
                    kind="fact",
                    content="Tim prefers Go",
                    derived_from=["event-1"],
                    extensions={"provider.example": {"label": "preference"}},
                )
            ],
        )

        restored = ExtractionProposal.from_json(proposal.to_json())

        self.assertEqual(restored.to_dict(), proposal.to_dict())
        self.assertEqual(
            restored.memories[0].extensions["provider.example"]["label"],
            "preference",
        )

    def test_unknown_proposal_source_is_rejected(self):
        proposal = ExtractionProposal(
            proposal_id="proposal-1",
            namespace="user:tim",
            actor="agent:extractor",
            memories=[
                MemoryProposal(
                    client_id="memory-1",
                    kind="fact",
                    content="invalid",
                    derived_from=["missing-event"],
                )
            ],
        )

        with self.assertRaises(ValueError):
            proposal.validate()


if __name__ == "__main__":
    unittest.main()
