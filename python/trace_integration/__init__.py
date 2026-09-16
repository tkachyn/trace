"""thin Python integrations for the Trace Go core"""

from .client import TraceClient, TraceClientError
from .http_client import HTTPTraceClient, TraceHTTPError
from .proposal import ExtractionProposal

__all__ = [
    "ExtractionProposal",
    "HTTPTraceClient",
    "TraceClient",
    "TraceClientError",
    "TraceHTTPError",
]
