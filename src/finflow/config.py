"""Central configuration.

Every environment variable the platform reads is declared here. Modules take a
``Settings`` instance rather than calling ``os.getenv`` directly, so the full
configuration surface is visible in one place and testable without patching the
environment.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(StrEnum):
    """Deployment environment."""

    LOCAL = "local"
    CI = "ci"
    PROD = "prod"


class LogLevel(StrEnum):
    """Supported logging levels."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class Settings(BaseSettings):
    """Runtime settings, populated from the environment or a ``.env`` file."""

    model_config = SettingsConfigDict(
        env_prefix="FINFLOW_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # ---- Runtime ---------------------------------------------------------
    env: Environment = Environment.LOCAL
    log_level: LogLevel = LogLevel.INFO
    log_json: bool | None = Field(
        default=None,
        description="Force JSON logging. When None, decided by TTY detection.",
    )

    # ---- Paths -----------------------------------------------------------
    data_dir: Path = Path("./data")
    duckdb_path: Path = Path("./data/finflow.duckdb")

    # ---- Data source credentials ----------------------------------------
    # Stooq requires no key, so it has no entry here.
    fred_api_key: SecretStr | None = None
    twelvedata_api_key: SecretStr | None = None
    alphavantage_api_key: SecretStr | None = None

    # ---- Alerts ----------------------------------------------------------
    telegram_bot_token: SecretStr | None = None
    telegram_chat_id: str | None = None

    # ---- Ingestion tuning ------------------------------------------------
    http_timeout_seconds: float = Field(default=30.0, gt=0)
    http_max_retries: int = Field(default=5, ge=0, le=10)

    @field_validator("data_dir", "duckdb_path")
    @classmethod
    def _expand(cls, value: Path) -> Path:
        """Resolve ``~`` so paths behave the same regardless of shell."""
        return value.expanduser()

    @property
    def raw_dir(self) -> Path:
        """Landing zone for raw ingested files."""
        return self.data_dir / "raw"

    @property
    def is_local(self) -> bool:
        """True when running on a developer machine."""
        return self.env is Environment.LOCAL

    def require(self, field: str) -> str:
        """Return a secret's value, or fail loudly if it is not configured.

        Credentials are optional at import time so that unrelated commands run
        without a full ``.env``. A component that genuinely needs one calls this
        and gets an actionable error instead of a downstream ``None``.
        """
        value = getattr(self, field, None)
        if value is None:
            raise MissingSettingError(field)
        return value.get_secret_value() if isinstance(value, SecretStr) else str(value)


class MissingSettingError(RuntimeError):
    """Raised when a required setting is absent from the environment."""

    def __init__(self, field: str) -> None:
        env_var = f"FINFLOW_{field.upper()}"
        super().__init__(
            f"Setting {field!r} is not configured. "
            f"Set {env_var} in your environment or .env file (see .env.example)."
        )
        self.field = field


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide settings, read from the environment once."""
    return Settings()
