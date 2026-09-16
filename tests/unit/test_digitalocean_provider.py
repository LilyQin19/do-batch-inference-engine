"""Unit tests for the live provider, entirely through respx at the httpx
transport level -- zero real network, as required everywhere in this suite.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from batchengine.providers.base import ProviderTransportError
from batchengine.providers.digitalocean import DigitalOceanProvider

_URL = "https://inference.example.com/v1/chat/completions"


@pytest.mark.asyncio
@respx.mock
async def test_complete_success_extracts_text_and_usage() -> None:
    respx.post(_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": "hello there"}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 5},
            },
            headers={"x-ratelimit-remaining-requests": "100"},
        )
    )
    async with httpx.AsyncClient() as client:
        provider = DigitalOceanProvider(
            client, "sk-fake", "https://inference.example.com/v1", "openai-gpt-oss-20b"
        )
        result = await provider.complete("hi", "openai-gpt-oss-20b", 64)

    assert result.status_code == 200
    assert result.text == "hello there"
    assert result.input_tokens == 12
    assert result.output_tokens == 5
    assert not result.malformed
    assert result.headers["x-ratelimit-remaining-requests"] == "100"


@pytest.mark.asyncio
@respx.mock
async def test_complete_propagates_non_200_status() -> None:
    respx.post(_URL).mock(return_value=httpx.Response(429, text="slow down"))
    async with httpx.AsyncClient() as client:
        provider = DigitalOceanProvider(
            client, "sk-fake", "https://inference.example.com/v1", "openai-gpt-oss-20b"
        )
        result = await provider.complete("hi", "openai-gpt-oss-20b", 64)
    assert result.status_code == 429


@pytest.mark.asyncio
@respx.mock
async def test_complete_marks_unparseable_200_as_malformed() -> None:
    respx.post(_URL).mock(return_value=httpx.Response(200, json={"unexpected": "shape"}))
    async with httpx.AsyncClient() as client:
        provider = DigitalOceanProvider(
            client, "sk-fake", "https://inference.example.com/v1", "openai-gpt-oss-20b"
        )
        result = await provider.complete("hi", "openai-gpt-oss-20b", 64)
    assert result.status_code == 200
    assert result.malformed


@pytest.mark.asyncio
@respx.mock
async def test_complete_raises_transport_error_on_timeout() -> None:
    respx.post(_URL).mock(side_effect=httpx.ConnectTimeout("boom"))
    async with httpx.AsyncClient() as client:
        provider = DigitalOceanProvider(
            client, "sk-fake", "https://inference.example.com/v1", "openai-gpt-oss-20b"
        )
        with pytest.raises(ProviderTransportError):
            await provider.complete("hi", "openai-gpt-oss-20b", 64)


def test_cost_pricing_matches_model_catalog() -> None:
    provider = DigitalOceanProvider(
        httpx.AsyncClient(), "sk-fake", "https://x", "openai-gpt-oss-20b"
    )
    assert provider.cost_per_1m_input() == 0.05
    assert provider.cost_per_1m_output() == 0.45


def test_cost_pricing_falls_back_for_unknown_model() -> None:
    provider = DigitalOceanProvider(
        httpx.AsyncClient(), "sk-fake", "https://x", "some-unknown-model"
    )
    assert provider.cost_per_1m_input() == 0.05
    assert provider.cost_per_1m_output() == 0.45
