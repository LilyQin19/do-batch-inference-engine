from __future__ import annotations

import hashlib
import hmac

import httpx
import pytest
import respx

from batchengine.extensions.webhook import (
    WebhookSSRFError,
    deliver_webhook,
    sign_payload,
    validate_webhook_url,
)


def test_sign_payload_is_deterministic_hmac_sha256() -> None:
    body = b'{"a": 1}'
    sig = sign_payload(body, "secret")
    expected = hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    assert sig == expected


def test_validate_rejects_non_http_scheme() -> None:
    with pytest.raises(WebhookSSRFError):
        validate_webhook_url("ftp://example.com/hook")


def test_validate_rejects_url_without_hostname() -> None:
    with pytest.raises(WebhookSSRFError):
        validate_webhook_url("http:///path")


def test_validate_rejects_loopback_by_default() -> None:
    with pytest.raises(WebhookSSRFError):
        validate_webhook_url("http://127.0.0.1:8000/hook")


def test_validate_rejects_link_local_metadata_address() -> None:
    with pytest.raises(WebhookSSRFError):
        validate_webhook_url("http://169.254.169.254/latest/meta-data")


def test_validate_allows_private_when_flag_set() -> None:
    validate_webhook_url("http://127.0.0.1:8000/hook", allow_private=True)


def test_validate_allows_public_hostname() -> None:
    validate_webhook_url("https://example.com/hook")


@pytest.mark.asyncio
@respx.mock
async def test_deliver_webhook_succeeds_on_2xx() -> None:
    route = respx.post("https://example.com/hook").mock(return_value=httpx.Response(200))
    async with httpx.AsyncClient() as client:
        ok = await deliver_webhook(client, "https://example.com/hook", {"a": 1}, "secret")
    assert ok
    assert route.called
    sent_headers = route.calls[0].request.headers
    assert "x-signature" in sent_headers


@pytest.mark.asyncio
@respx.mock
async def test_deliver_webhook_retries_then_gives_up_on_persistent_failure() -> None:
    route = respx.post("https://example.com/hook").mock(return_value=httpx.Response(500))
    async with httpx.AsyncClient() as client:
        ok = await deliver_webhook(client, "https://example.com/hook", {"a": 1}, "secret")
    assert not ok
    assert route.call_count == 3


@pytest.mark.asyncio
async def test_deliver_webhook_raises_on_ssrf_target() -> None:
    async with httpx.AsyncClient() as client:
        with pytest.raises(WebhookSSRFError):
            await deliver_webhook(client, "http://127.0.0.1/hook", {}, "secret")
