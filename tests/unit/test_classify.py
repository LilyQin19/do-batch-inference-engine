from __future__ import annotations

import pytest

from batchengine.core.models import FailureClass
from batchengine.core.retry import classify
from batchengine.providers.base import ProviderResponse, ProviderTransportError

# Table-driven over the entire §6.2 taxonomy.
_CASES = [
    (200, FailureClass.SUCCESS, 1),
    (429, FailureClass.THROTTLED, 8),
    (408, FailureClass.TRANSIENT, 5),
    (500, FailureClass.TRANSIENT, 5),
    (502, FailureClass.TRANSIENT, 5),
    (503, FailureClass.TRANSIENT, 5),
    (504, FailureClass.TRANSIENT, 5),
    (400, FailureClass.INVALID_INPUT, 1),
    (422, FailureClass.INVALID_INPUT, 1),
    (413, FailureClass.INVALID_INPUT, 1),
    (401, FailureClass.FATAL_AUTH, 1),
    (403, FailureClass.FATAL_AUTH, 1),
    (402, FailureClass.FATAL_BILLING, 1),
]


@pytest.mark.parametrize("status,expected_class,expected_max_attempts", _CASES)
def test_classify_http_status_table(
    status: int, expected_class: FailureClass, expected_max_attempts: int
) -> None:
    response = ProviderResponse(status_code=status)
    failure_class, max_attempts = classify(response)
    assert failure_class == expected_class
    assert max_attempts == expected_max_attempts


def test_classify_transport_error_is_transient() -> None:
    failure_class, max_attempts = classify(None, exc=ProviderTransportError("boom"))
    assert failure_class == FailureClass.TRANSIENT
    assert max_attempts == 5


def test_classify_malformed_response_retries_once_then_terminal() -> None:
    response = ProviderResponse(status_code=200, malformed=True)
    failure_class, max_attempts = classify(response)
    assert failure_class == FailureClass.TRANSIENT
    assert max_attempts == 2


def test_classify_unknown_status_is_conservatively_transient() -> None:
    response = ProviderResponse(status_code=418)
    failure_class, max_attempts = classify(response)
    assert failure_class == FailureClass.TRANSIENT


def test_fatal_classes_flagged_correctly() -> None:
    assert FailureClass.FATAL_AUTH.is_fatal
    assert FailureClass.FATAL_BILLING.is_fatal
    assert not FailureClass.TRANSIENT.is_fatal
    assert not FailureClass.THROTTLED.is_fatal
    assert FailureClass.THROTTLED.is_retryable
    assert FailureClass.TRANSIENT.is_retryable
    assert not FailureClass.INVALID_INPUT.is_retryable
