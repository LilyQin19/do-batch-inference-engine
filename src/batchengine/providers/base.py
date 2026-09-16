"""Provider abstraction. The scheduler/worker never talks HTTP directly --
it only knows this Protocol, which is why CI needs no network and no secrets:
the mock provider (providers/mock.py) satisfies it with zero I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


class ProviderTransportError(Exception):
    """Connection/timeout failure -- no HTTP response was received at all.
    Always classified as `transient` (§6.2): the network hiccup carries no
    information about whether the request itself was valid.
    """


@dataclass(slots=True)
class ProviderResponse:
    """Normalized result of one inference call, whatever transport produced it."""

    status_code: int
    text: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    malformed: bool = False
    headers: dict[str, str] = field(default_factory=dict)


class InferenceProvider(Protocol):
    """Anything the scheduler can call. `digitalocean.py` and `mock.py` both
    implement this; the scheduler is written against the Protocol only.
    """

    async def complete(self, prompt: str, model: str, max_tokens: int) -> ProviderResponse:
        """Perform one completion call.

        Raises ProviderTransportError on connection/timeout failure.
        Returns a ProviderResponse (any status_code) otherwise -- HTTP-level
        errors (429/500/400/...) are not exceptions, they're data for
        retry.classify() to interpret.
        """
        ...

    def cost_per_1m_input(self) -> float:
        """USD per 1M input tokens, for cost accounting."""
        ...

    def cost_per_1m_output(self) -> float:
        """USD per 1M output tokens, for cost accounting."""
        ...
