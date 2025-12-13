# cortex-utils

Operational utilities for Cortex: queue management, alerting, and log analysis.

## Features

- **Queue Management**: Partitioned queue table with automatic cleanup
- **Dead Letter Queue**: Archive and retry failed jobs
- **Alerter**: Discord webhook notifications for errors (planned)
- **Log Analysis**: Aggregate and search Docker logs (planned)

## Installation

```bash
uv sync
```

## Usage

```bash
# Queue statistics
cortex-utils queue stats

# Partition management
cortex-utils partitions list
cortex-utils partitions maintain --retention-days 7

# Dead letter management
cortex-utils dead-letter list
cortex-utils dead-letter retry --id 123

# Migration (one-time)
cortex-utils migrate-queue --dry-run
cortex-utils migrate-queue --execute
```

## Configuration

Set environment variables:

```bash
export POSTGRES_HOST=10.5.2.21
export POSTGRES_DB=cortex
export POSTGRES_USER=cortex
export POSTGRES_PASSWORD=secret
```

Or use a config file at `~/.cortex/utils.yaml`.

## Development

```bash
uv sync
uv run pytest
uv run mypy src/
uv run black .
uv run ruff check .
```

## License

MIT
