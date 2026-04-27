"""Production-grade async HTTP client with resilience primitives.

Public surface is re-exported lazily as components are added. Only imports
that exist at this commit are exposed; later commits will append to
`__all__` as the client, queue, and recovery layers come online.
"""

from async_hardened_client.config import (
    CircuitBreakerConfig,
    ClientConfig,
    QueueConfig,
    RateLimitConfig,
    RetryPolicy,
)
from async_hardened_client.errors import (
    AsyncHardenedError,
    CircuitOpenError,
    DeadLetterError,
    NonRetryableError,
    RetryableError,
)
from async_hardened_client.models import Request, Response

__version__ = "0.1.0"

__all__ = [
    "CircuitBreakerConfig",
    "ClientConfig",
    "QueueConfig",
    "RateLimitConfig",
    "RetryPolicy",
    "Request",
    "Response",
    "AsyncHardenedError",
    "CircuitOpenError",
    "DeadLetterError",
    "NonRetryableError",
    "RetryableError",
    "__version__",
]
