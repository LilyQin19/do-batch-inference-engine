"""Terminal-state webhook delivery (§8.2). Signed with HMAC-SHA256 so the
receiver can verify the payload actually came from this service, retried
with full jitter like any other transient failure, and SSRF-guarded because
"POST to a URL the caller supplies" is a classic way to make a server issue
requests to its own internal network on the caller's behalf.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import socket
from urllib.parse import urlparse

import httpx
import structlog

from batchengine.core.retry import full_jitter_delay

log = structlog.get_logger()

_MAX_ATTEMPTS = 3


class WebhookSSRFError(ValueError):
    pass


def validate_webhook_url(url: str, allow_private: bool = False) -> None:
    """Reject URLs that resolve to private/loopback/link-local ranges unless
    explicitly allowed. Resolving the hostname (rather than only checking its
    literal form) matters -- `http://attacker.example` can have a DNS record
    pointing straight at 169.254.169.254 (a cloud metadata endpoint).
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise WebhookSSRFError(f"unsupported scheme: {parsed.scheme!r}")
    if not parsed.hostname:
        raise WebhookSSRFError("webhook URL has no hostname")
    if allow_private:
        return
    try:
        infos = socket.getaddrinfo(parsed.hostname, None)
    except socket.gaierror as exc:
        raise WebhookSSRFError(f"could not resolve webhook host: {exc}") from exc
    for _family, _, _, _, sockaddr in infos:
        ip = ipaddress.ip_address(sockaddr[0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
            raise WebhookSSRFError(f"webhook host resolves to a disallowed address: {ip}")


def sign_payload(payload: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


async def deliver_webhook(
    client: httpx.AsyncClient,
    url: str,
    payload: dict[str, object],
    secret: str,
    allow_private: bool = False,
) -> bool:
    validate_webhook_url(url, allow_private=allow_private)
    body = json.dumps(payload).encode()
    signature = sign_payload(body, secret)
    headers = {"Content-Type": "application/json", "X-Signature": signature}

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            resp = await client.post(url, content=body, headers=headers, timeout=10.0)
            if resp.status_code < 300:
                return True
            log.warning("webhook.non_2xx", url=url, status=resp.status_code, attempt=attempt)
        except httpx.HTTPError as exc:
            log.warning("webhook.transport_error", url=url, error=str(exc), attempt=attempt)
        if attempt < _MAX_ATTEMPTS:
            import asyncio

            await asyncio.sleep(full_jitter_delay(attempt))
    return False
