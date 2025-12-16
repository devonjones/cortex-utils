# cortex-utils

Operational utilities for Cortex: queue management, alerting, log tools.

## Features

- **Queue Management**: Partitioned queue with automatic maintenance
- **Dead Letter**: Failed job tracking and retry
- **Discord Alerter**: Log tailing with error classification and alerts

## Usage

```bash
# Queue stats
cortex-utils queue stats

# Partition maintenance
cortex-utils partitions maintain --retention-days 7

# Run alerter daemon
DISCORD_WEBHOOK_URL=... cortex-utils alerter run

# Send test alert
DISCORD_WEBHOOK_URL=... cortex-utils alerter test
```

## Environment Variables

- `POSTGRES_HOST`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` - Database
- `DISCORD_WEBHOOK_URL` - Discord webhook for alerts
