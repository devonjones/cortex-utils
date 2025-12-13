"""Configuration loading for cortex-utils."""

import os
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class PostgresConfig:
    """Postgres connection configuration."""

    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        """Return connection string."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password}"
        )


@dataclass
class Config:
    """Application configuration."""

    postgres: PostgresConfig
    discord_webhook_url: str | None = None
    queue_retention_days: int = 7
    dead_letter_retention_days: int = 30

    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        postgres = PostgresConfig(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            database=os.environ.get("POSTGRES_DB", "cortex"),
            user=os.environ.get("POSTGRES_USER", "cortex"),
            password=os.environ.get("POSTGRES_PASSWORD", ""),
        )

        return cls(
            postgres=postgres,
            discord_webhook_url=os.environ.get("DISCORD_WEBHOOK_URL"),
            queue_retention_days=int(os.environ.get("QUEUE_RETENTION_DAYS", "7")),
            dead_letter_retention_days=int(os.environ.get("DEAD_LETTER_RETENTION_DAYS", "30")),
        )

    @classmethod
    def from_file(cls, path: Path) -> "Config":
        """Load configuration from YAML file, with env var overrides."""
        config = cls.from_env()

        if path.exists():
            with open(path) as f:
                data = yaml.safe_load(f)

            if data and "postgres" in data:
                pg = data["postgres"]
                if not config.postgres.password:
                    config.postgres = PostgresConfig(
                        host=pg.get("host", config.postgres.host),
                        port=pg.get("port", config.postgres.port),
                        database=pg.get("database", config.postgres.database),
                        user=pg.get("user", config.postgres.user),
                        password=pg.get("password", config.postgres.password),
                    )

            if data and "retention" in data:
                ret = data["retention"]
                config.queue_retention_days = ret.get("queue_days", config.queue_retention_days)
                config.dead_letter_retention_days = ret.get(
                    "dead_letter_days", config.dead_letter_retention_days
                )

        return config
