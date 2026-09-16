import json
import unittest
from types import SimpleNamespace

from trace_integration.client import TraceClient, TraceClientError
from trace_integration.proposal import ExtractionProposal


class FakeRunner:
    def __init__(self, returncode=0, result=None):
        self.returncode = returncode
        self.result = result or {"commit_id": "commit-1", "accepted": ["record-1"]}
        self.command = None
        self.input = None

    def __call__(self, command, input, **kwargs):
        self.command = command
        self.input = input
        return SimpleNamespace(
            returncode=self.returncode,
            stdout=json.dumps(self.result),
            stderr="ingest failed" if self.returncode else "",
        )


class ClientTests(unittest.TestCase):
    def proposal(self):
        return ExtractionProposal(
            proposal_id="proposal-1",
            namespace="user:tim",
            actor="agent:test",
        )

    def test_client_uses_ingest_subprocess(self):
        runner = FakeRunner()
        result = TraceClient("trace-test", runner=runner).ingest_proposal(
            "memory.trc",
            self.proposal(),
        )

        self.assertEqual(result["commit_id"], "commit-1")
        self.assertEqual(runner.command, ["trace-test", "ingest", "memory.trc"])
        self.assertNotIn("sqlite", runner.input.lower())

    def test_client_supports_jsonl(self):
        runner = FakeRunner()
        TraceClient(runner=runner).ingest_proposal(
            "memory.trc",
            self.proposal(),
            jsonl=True,
        )

        self.assertEqual(runner.command[-2], "--jsonl")
        self.assertIn('"type": "header"', runner.input)
        self.assertIn('"type": "commit"', runner.input)

    def test_client_reports_process_failures(self):
        runner = FakeRunner(returncode=1)

        with self.assertRaises(TraceClientError):
            TraceClient(runner=runner).ingest_proposal(
                "memory.trc",
                self.proposal(),
            )


if __name__ == "__main__":
    unittest.main()
