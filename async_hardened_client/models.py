"""Core data models for requests and responses."""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


def _stable_dump(value: Any) -> str:
    """JSON-encode a value with sorted keys so the same payload always hashes
    to the same string regardless of dict insertion order.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


@dataclass(frozen=True)
class Request:
    """An immutable description of an HTTP request.

    `idempotency_key` is the canonical hash used by the deduplication layer
    and as the persistent storage primary lookup key. Two requests with the
    same method, URL, params, and body share an idempotency key and resolve
    once.
    """

    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    body: Any | None = None
    priority: int = 0
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    created_at: float = field(default_factory=time.time)

    @property
    def idempotency_key(self) -> str:
        payload = {
            "method": self.method.upper(),
            "url": self.url,
            "params": self.params,
            "body": self.body,
        }
        digest = hashlib.sha256(_stable_dump(payload).encode("utf-8")).hexdigest()
        return digest

    def to_storage(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dict for persistence."""
        return {
            "method": self.method,
            "url": self.url,
            "headers": self.headers,
            "params": self.params,
            "body": self.body,
            "priority": self.priority,
            "request_id": self.request_id,
            "created_at": self.created_at,
        }

    @classmethod
    def from_storage(cls, data: dict[str, Any]) -> Request:
        return cls(
            method=data["method"],
            url=data["url"],
            headers=data.get("headers", {}) or {},
            params=data.get("params", {}) or {},
            body=data.get("body"),
            priority=data.get("priority", 0),
            request_id=data.get("request_id", uuid.uuid4().hex),
            created_at=data.get("created_at", time.time()),
        )


@dataclass(frozen=True)
class Response:
    """Result of an executed request.

    `error` is set only on terminal non-retryable failure. A successful
    response has `status_code` set and `error` empty; a request that landed
    in the dead-letter queue surfaces here as an exception, not a Response.
    """

    status_code: int
    data: Any
    headers: dict[str, str] = field(default_factory=dict)
    error: str | None = None
    retries: int = 0
    request_id: str = ""
