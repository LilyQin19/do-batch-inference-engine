"""Live DigitalOcean Serverless Inference provider.

Implemented completely but never exercised by CI or the test suite -- see
README "Product findings" and STATUS.md. It is exercised only when
`DO_INFERENCE_KEY` is present and a caller explicitly runs a live job, which
did not happen during this build (no key was provisioned).
"""

from __future__ import annotations

import httpx

from batchengine.providers.base import ProviderResponse, ProviderTransportError

_RATE_LIMIT_HEADERS = (
    "x-ratelimit-limit-requests",
    "x-ratelimit-remaining-requests",
    "x-ratelimit-reset-requests",
    "x-ratelimit-limit-tokens-per-minute",
    "x-ratelimit-remaining-tokens-per-minute",
    "x-ratelimit-reset-tokens-per-minute",
    "x-ratelimit-limit-tokens-per-day",
    "x-ratelimit-remaining-tokens-per-day",
    "x-ratelimit-reset-tokens-per-day",
)

# $/1M tokens, serverless catalog as researched 2026-09-15 (§1.1 of instructions.md).
# Kept here (not hardcoded into cost math) so README's cost table and the
# scheduler's spend guard read from one source.
MODEL_PRICING: dict[str, tuple[float, float]] = {
    "openai-gpt-oss-20b": (0.05, 0.45),
    "openai-gpt-oss-120b": (0.06, 0.39),
    "gemma-4-31B-it": (0.18, 0.50),
    "llama-4-maverick": (0.20, 0.696),
    "ministral-3-14B": (0.20, 0.20),
}
_DEFAULT_PRICING = MODEL_PRICING["openai-gpt-oss-20b"]


class DigitalOceanProvider:
    """Implements InferenceProvider against the OpenAI-compatible chat
    completions endpoint DigitalOcean Serverless Inference exposes.
    """

    def __init__(self, client: httpx.AsyncClient, api_key: str, base_url: str, model_for_pricing: str) -> None:
        self._client = client
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._pricing = MODEL_PRICING.get(model_for_pricing, _DEFAULT_PRICING)

    async def complete(self, prompt: str, model: str, max_tokens: int) -> ProviderResponse:
        try:
            resp = await self._client.post(
                f"{self._base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self._api_key}"},
                json={
                    "model": model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                },
            )
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
            raise ProviderTransportError(str(exc)) from exc

        headers = {h: resp.headers[h] for h in _RATE_LIMIT_HEADERS if h in resp.headers}

        if resp.status_code != 200:
            return ProviderResponse(status_code=resp.status_code, text=resp.text, headers=headers)

        try:
            body = resp.json()
            text = body["choices"][0]["message"]["content"]
            usage = body.get("usage", {})
            input_tokens = int(usage.get("prompt_tokens", 0))
            output_tokens = int(usage.get("completion_tokens", 0))
        except (KeyError, IndexError, ValueError, TypeError):
            return ProviderResponse(status_code=200, malformed=True, headers=headers)

        return ProviderResponse(
            status_code=200,
            text=text,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            headers=headers,
        )

    def cost_per_1m_input(self) -> float:
        return self._pricing[0]

    def cost_per_1m_output(self) -> float:
        return self._pricing[1]
