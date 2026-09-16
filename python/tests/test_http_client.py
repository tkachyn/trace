import json
import unittest
from unittest.mock import patch

from trace_integration.http_client import HTTPTraceClient, TraceHTTPError


class FakeResponse:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return self.payload


class HTTPClientTests(unittest.TestCase):
    @patch("trace_integration.http_client.request.urlopen")
    def test_search_sends_principal_and_returns_json(self, urlopen):
        urlopen.return_value = FakeResponse({"EvidenceState": "supported"})
        client = HTTPTraceClient(
            "http://127.0.0.1:8080",
            principal="agent:test",
            purpose="answer",
        )

        result = client.search(text="prefers Go")

        self.assertEqual(result["EvidenceState"], "supported")
        sent = urlopen.call_args.args[0]
        self.assertEqual(sent.full_url, "http://127.0.0.1:8080/v1/search")
        payload = json.loads(sent.data.decode("utf-8"))
        self.assertEqual(payload["principal"], "agent:test")
        self.assertEqual(payload["purpose"], "answer")

    @patch("trace_integration.http_client.request.urlopen")
    def test_http_error_preserves_category_and_status(self, urlopen):
        from urllib.error import HTTPError

        failure = HTTPError(
            "http://127.0.0.1:8080/v1/search",
            403,
            "denied",
            {},
            None,
        )
        failure.read = lambda: b'{"error":{"category":"unauthorized","message":"denied"}}'
        urlopen.side_effect = failure

        with self.assertRaises(TraceHTTPError) as context:
            HTTPTraceClient("http://127.0.0.1:8080").search()

        self.assertEqual(context.exception.status, 403)
        self.assertEqual(context.exception.category, "unauthorized")


if __name__ == "__main__":
    unittest.main()
