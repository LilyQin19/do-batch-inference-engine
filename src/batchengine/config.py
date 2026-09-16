"""Process-wide configuration via environment variables (pydantic-settings).

Nothing here is a secret except DO_INFERENCE_KEY, which is Optional and unset
in CI. The test suite never needs a .env file.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    do_inference_key: str | None = None
    do_inference_base_url: str = "https://inference.do-ai.run/v1"

    batchengine_model: str = "mistral-3-14B"
    batchengine_max_tokens: int = 128
    batchengine_rate_limit_rpm: int = 120
    # §6.2 default is 30s. Overridable so the test suite can shrink it --
    # the breaker's *logic* is exercised with a fake clock in
    # tests/unit/test_retry.py; this only controls how long a real,
    # HTTP-driven test has to wait out an open breaker in wall-clock time.
    batchengine_circuit_cooldown_s: float = 30.0

    batchengine_max_job_spend_usd: float = 0.25
    batchengine_max_total_spend_usd: float = 1.00
    batchengine_live_sample_size: int = 50
    batchengine_spend_ledger_path: str = "./.spend_ledger.json"

    batchengine_db_path: str = "./data/jobs.db"
    batchengine_results_dir: str = "./data/results"

    batchengine_spaces_enabled: bool = False
    spaces_key: str | None = None
    spaces_secret: str | None = None
    spaces_bucket: str | None = None
    spaces_region: str = "nyc3"

    batchengine_allow_private_webhooks: bool = False

    # Hard safety rail (§12.2): never run a live job above this many items,
    # regardless of what the request asks for.
    batchengine_max_live_items: int = 1000


def get_settings() -> Settings:
    return Settings()
