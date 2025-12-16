# cortex-utils - Shared Utilities & Operations

**Project**: cortex-utils
**Purpose**: Shared operational tooling for queue management, log aggregation, alerting, and maintenance

## Overview

cortex-utils provides cross-cutting operational capabilities for all Cortex services. It consolidates queue management, partition maintenance, log analysis, and Discord alerting into a single CLI tool and set of services.

## Design Philosophy

- **Single responsibility**: Each tool does one thing well
- **Composable**: CLI commands can be piped and scripted
- **Safe defaults**: Destructive operations require confirmation
- **Observable**: All operations emit structured logs

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Cortex Services (Hades)                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────────┐│
│  │gmail-sync│ │ triage   │ │ labeling │ │ parse/attach workers ││
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └──────────┬───────────┘│
│       │            │            │                   │            │
│       └────────────┴────────────┴───────────────────┘            │
│                              │                                   │
│              Postgres (queue table, partitioned)                 │
│              Docker logs (stderr/stdout)                         │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│                        cortex-utils                              │
│  ┌────────────────┐ ┌────────────────┐ ┌──────────────────────┐ │
│  │ Queue Manager  │ │  Log Analyzer  │ │      Alerter         │ │
│  │ - partitions   │ │ - tail/search  │ │ - error detection    │ │
│  │ - replay       │ │ - aggregation  │ │ - Discord webhook    │ │
│  │ - dead letter  │ │ - export       │ │ - rate limiting      │ │
│  └────────────────┘ └────────────────┘ └──────────────────────┘ │
│                              │                                   │
│                         CLI (click)                              │
└──────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Queue Manager

Manages the partitioned queue table and job lifecycle.

#### Partition Management

```python
# Daily cron job: create future partitions, drop old ones
cortex-utils partitions maintain --retention-days 7

# Manual operations
cortex-utils partitions list
cortex-utils partitions create --date 2024-12-20
cortex-utils partitions drop --date 2024-12-06 --force
```

**Partition schema:**
```sql
-- Partitioned queue table
CREATE TABLE queue (
    id BIGSERIAL,
    queue_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT DEFAULT 0,
    max_attempts INT DEFAULT 3,
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    CONSTRAINT valid_status CHECK (
        status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')
    ),
    PRIMARY KEY (id, created_at)  -- partition key must be in PK
) PARTITION BY RANGE (created_at);

-- Indexes (created on each partition automatically)
CREATE INDEX idx_queue_pending ON queue(queue_name, status, created_at)
    WHERE status = 'pending';
CREATE INDEX idx_queue_processing ON queue(queue_name, claimed_at)
    WHERE status = 'processing';
```

**Partition naming convention:**
```
queue_YYYY_MM_DD  -- e.g., queue_2024_12_13
```

**Daily maintenance cron (via Ofelia):**
```yaml
jobs:
  - name: cortex-partition-maintenance
    schedule: "0 2 * * *"  # 2 AM daily
    command: >
      docker exec cortex-utils
      cortex-utils partitions maintain --retention-days 7
```

**Maintenance logic:**
1. Create partitions for next 3 days (idempotent)
2. Archive failed jobs from partition to be dropped → `dead_letter` table
3. Drop partitions older than retention period
4. Log partition sizes and counts

#### Dead Letter Management

Failed jobs are preserved before partition drops:

```sql
CREATE TABLE dead_letter (
    id BIGSERIAL PRIMARY KEY,
    original_id BIGINT NOT NULL,
    queue_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INT NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,      -- original creation time
    failed_at TIMESTAMPTZ NOT NULL,       -- when moved to dead letter
    archived_from_partition TEXT NOT NULL -- source partition name
);

CREATE INDEX idx_dead_letter_queue ON dead_letter(queue_name, failed_at DESC);
```

**CLI:**
```bash
# List dead letter jobs
cortex-utils dead-letter list
cortex-utils dead-letter list --queue triage --since 7d

# Inspect a specific job
cortex-utils dead-letter show 12345

# Retry failed jobs
cortex-utils dead-letter retry --queue triage --since 24h
cortex-utils dead-letter retry --id 12345

# Purge old dead letter entries
cortex-utils dead-letter purge --older-than 30d
```

#### Queue Replay

Re-enqueue emails for reprocessing:

```bash
# Replay by label (emails currently labeled X)
cortex-utils queue replay --label "Cortex/Commercial" --to-queue triage

# Replay by date range
cortex-utils queue replay --since 2024-12-01 --until 2024-12-07 --to-queue triage

# Replay specific emails
cortex-utils queue replay --gmail-ids abc123,def456 --to-queue triage

# Dry run (show what would be enqueued)
cortex-utils queue replay --label "Cortex/Unknown" --to-queue triage --dry-run
```

#### Queue Stats

```bash
# Current queue depths
cortex-utils queue stats

# Output:
# Queue         Pending  Processing  Completed (24h)  Failed (24h)
# -----------   -------  ----------  ---------------  ------------
# triage             0           0             1,234            12
# labeling           0           0             1,189             3
# parse              0           0             1,234             0
# attachment         0           0               156             1
# actions        4,069           0                 0             0

# Historical throughput
cortex-utils queue stats --history 7d
```

### 2. Log Analyzer

Aggregates and analyzes logs from all Cortex containers.

#### Log Tailing

```bash
# Tail all cortex containers
cortex-utils logs tail

# Tail specific services
cortex-utils logs tail --services gmail-sync,triage-worker

# Filter by level
cortex-utils logs tail --level error

# Follow mode (like tail -f)
cortex-utils logs tail -f --services gmail-sync
```

#### Error Search

```bash
# Find errors in last hour
cortex-utils logs errors --since 1h

# Find specific patterns
cortex-utils logs search "History expired" --since 24h

# Export for analysis
cortex-utils logs export --since 24h --format jsonl > logs.jsonl
```

### 3. Alerter

Real-time error detection and Discord notification. See [alerter-spec.md](../../docs/alerter-spec.md) for full design.

**Integration with cortex-utils:**
```bash
# Run alerter daemon
cortex-utils alerter run

# Test Discord webhook
cortex-utils alerter test

# Send manual alert
cortex-utils alerter send --level critical --message "Manual alert test"

# Show alert history
cortex-utils alerter history --since 24h
```

### 4. Health Checker

Cross-service health monitoring:

```bash
# Check all services
cortex-utils health check

# Output:
# Service              Status    Latency    Notes
# ------------------   ------    -------    -----
# cortex-postgres      OK        2ms
# cortex-gmail-sync    OK        -          pid 1234, uptime 3d
# cortex-duckdb-api    OK        15ms       8,718 bodies stored
# cortex-triage-worker OK        -          pid 1235, uptime 3d
# cortex-labeling      OK        -          pid 1236, uptime 3d
# ollama               OK        145ms      qwen2.5:0.5b loaded

# JSON output for monitoring systems
cortex-utils health check --format json
```

## Project Structure

```
cortex-utils/
├── src/cortex_utils/
│   ├── __init__.py
│   ├── cli.py                    # Main CLI entrypoint (click)
│   ├── config.py                 # Configuration loading
│   │
│   ├── queue/
│   │   ├── __init__.py
│   │   ├── partitions.py         # Partition create/drop/maintain
│   │   ├── dead_letter.py        # Dead letter management
│   │   ├── replay.py             # Queue replay functionality
│   │   └── stats.py              # Queue statistics
│   │
│   ├── logs/
│   │   ├── __init__.py
│   │   ├── tailer.py             # Docker log tailing
│   │   ├── search.py             # Log search/filtering
│   │   └── export.py             # Log export
│   │
│   ├── alerter/
│   │   ├── __init__.py
│   │   ├── daemon.py             # Main alerter loop
│   │   ├── classifier.py         # Error pattern matching
│   │   ├── discord.py            # Discord webhook client
│   │   └── rate_limiter.py       # Alert deduplication
│   │
│   └── health/
│       ├── __init__.py
│       └── checker.py            # Service health checks
│
├── scripts/
│   ├── migrate_to_partitioned.py # One-time migration script
│   └── setup_cron.sh             # Install Ofelia job
│
├── tests/
│   ├── test_partitions.py
│   ├── test_dead_letter.py
│   ├── test_replay.py
│   └── test_alerter.py
│
├── docs/
│   └── project-spec.md           # This file
│
├── pyproject.toml
├── Dockerfile
└── CLAUDE.md
```

## Configuration

### Environment Variables

```bash
# Postgres (required)
POSTGRES_HOST=10.5.2.21
POSTGRES_PORT=5432
POSTGRES_DB=cortex
POSTGRES_USER=cortex
POSTGRES_PASSWORD=<secret>

# Docker (for log tailing)
DOCKER_HOST=unix:///var/run/docker.sock
# Or for remote: DOCKER_HOST=tcp://10.5.2.21:2375

# Discord (for alerter)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxx/yyy

# Ollama (for health checks)
OLLAMA_URL=http://10.5.2.12:11434

# Retention settings
QUEUE_RETENTION_DAYS=7
DEAD_LETTER_RETENTION_DAYS=30
```

### Config File (optional)

```yaml
# ~/.cortex/utils.yaml
postgres:
  host: 10.5.2.21
  port: 5432
  database: cortex
  user: cortex

docker:
  host: unix:///var/run/docker.sock

alerter:
  discord_webhook_url: ${DISCORD_WEBHOOK_URL}
  cooldown_minutes: 5
  daily_summary_hour: 0

retention:
  queue_days: 7
  dead_letter_days: 30
```

## Migration: Non-Partitioned to Partitioned Queue

**One-time migration script** (`scripts/migrate_to_partitioned.py`):

```python
"""
Migrate existing queue table to partitioned design.

Steps:
1. Create new partitioned table (queue_new)
2. Create partitions for existing data date range + future
3. Copy data from old table to new
4. Rename tables (queue → queue_old, queue_new → queue)
5. Verify counts match
6. Drop old table after confirmation

Run with: cortex-utils migrate-queue --dry-run
Then:      cortex-utils migrate-queue --execute
"""

def migrate_queue(dry_run: bool = True):
    conn = get_connection()

    # 1. Analyze existing data
    result = conn.execute("""
        SELECT
            MIN(created_at)::date as min_date,
            MAX(created_at)::date as max_date,
            COUNT(*) as total_rows
        FROM queue
    """).fetchone()

    min_date, max_date, total_rows = result
    print(f"Existing data: {total_rows} rows from {min_date} to {max_date}")

    if dry_run:
        print("Dry run - no changes made")
        return

    # 2. Create partitioned table
    conn.execute("""
        CREATE TABLE queue_new (
            id BIGSERIAL,
            queue_name TEXT NOT NULL,
            payload JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INT DEFAULT 0,
            max_attempts INT DEFAULT 3,
            last_error TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            claimed_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ,
            CONSTRAINT valid_status CHECK (
                status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')
            ),
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at);
    """)

    # 3. Create partitions for date range
    current = min_date
    while current <= max_date + timedelta(days=7):  # Include future
        partition_name = f"queue_{current.strftime('%Y_%m_%d')}"
        next_date = current + timedelta(days=1)
        conn.execute(f"""
            CREATE TABLE {partition_name} PARTITION OF queue_new
            FOR VALUES FROM ('{current}') TO ('{next_date}');
        """)
        current = next_date

    # 4. Copy data
    conn.execute("""
        INSERT INTO queue_new
        SELECT * FROM queue;
    """)

    # 5. Verify
    new_count = conn.execute("SELECT COUNT(*) FROM queue_new").fetchone()[0]
    assert new_count == total_rows, f"Count mismatch: {new_count} vs {total_rows}"

    # 6. Swap tables
    conn.execute("ALTER TABLE queue RENAME TO queue_old;")
    conn.execute("ALTER TABLE queue_new RENAME TO queue;")

    # 7. Recreate indexes
    conn.execute("""
        CREATE INDEX idx_queue_pending ON queue(queue_name, status, created_at)
            WHERE status = 'pending';
        CREATE INDEX idx_queue_processing ON queue(queue_name, claimed_at)
            WHERE status = 'processing';
    """)

    conn.commit()
    print(f"Migration complete. Old table preserved as 'queue_old'.")
    print("After verification, drop with: DROP TABLE queue_old;")
```

**Migration checklist:**
- [ ] Backup database before migration
- [ ] Run migration during low-traffic period
- [ ] Verify all services reconnect properly
- [ ] Monitor for errors for 24 hours
- [ ] Drop `queue_old` after verification

## Deployment

### Docker Image

```dockerfile
FROM python:3.13-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies
RUN uv sync --frozen --no-dev

# Copy source
COPY src/ ./src/

# Need docker socket access for log tailing
VOLUME /var/run/docker.sock

ENTRYPOINT ["uv", "run", "cortex-utils"]
```

### Portainer Stack

```yaml
version: '3.8'
services:
  utils:
    image: us-central1-docker.pkg.dev/cortex-gmail/cortex/utils:latest
    container_name: cortex-utils
    restart: "no"  # Run on-demand, not continuously
    environment:
      - POSTGRES_HOST=${POSTGRES_HOST}
      - POSTGRES_DB=${POSTGRES_DB:-cortex}
      - POSTGRES_USER=${POSTGRES_USER:-cortex}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - DISCORD_WEBHOOK_URL=${DISCORD_WEBHOOK_URL}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks:
      - cortex

  alerter:
    image: us-central1-docker.pkg.dev/cortex-gmail/cortex/utils:latest
    container_name: cortex-alerter
    command: ["alerter", "run"]
    restart: unless-stopped
    environment:
      - POSTGRES_HOST=${POSTGRES_HOST}
      - POSTGRES_DB=${POSTGRES_DB:-cortex}
      - POSTGRES_USER=${POSTGRES_USER:-cortex}
      - POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      - DISCORD_WEBHOOK_URL=${DISCORD_WEBHOOK_URL}
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
    networks:
      - cortex

networks:
  cortex:
    external: true
```

### Cron Jobs (via Ofelia on Hades)

```yaml
jobs:
  # Daily partition maintenance at 2 AM
  - name: cortex-partition-maintenance
    schedule: "0 2 * * *"
    command: >
      docker run --rm
      --network cortex
      -e POSTGRES_HOST=${POSTGRES_HOST}
      -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      us-central1-docker.pkg.dev/cortex-gmail/cortex/utils:latest
      partitions maintain --retention-days 7

  # Daily dead letter cleanup at 3 AM
  - name: cortex-dead-letter-cleanup
    schedule: "0 3 * * *"
    command: >
      docker run --rm
      --network cortex
      -e POSTGRES_HOST=${POSTGRES_HOST}
      -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
      us-central1-docker.pkg.dev/cortex-gmail/cortex/utils:latest
      dead-letter purge --older-than 30d --yes
```

## CLI Reference

```
cortex-utils - Cortex operational utilities

Usage: cortex-utils [OPTIONS] COMMAND [ARGS]...

Commands:
  partitions   Manage queue table partitions
  queue        Queue operations (stats, replay)
  dead-letter  Dead letter queue management
  logs         Log tailing and analysis
  alerter      Discord alerting
  health       Service health checks
  migrate-queue  One-time migration to partitioned queue

Options:
  --config PATH  Config file path (default: ~/.cortex/utils.yaml)
  --verbose      Enable debug logging
  --help         Show this message and exit

Examples:
  cortex-utils queue stats
  cortex-utils partitions maintain --retention-days 7
  cortex-utils logs tail -f --services gmail-sync
  cortex-utils alerter run
```

## Implementation Phases

### Phase 1: Core Queue Management
- [x] Project setup (pyproject.toml, structure)
- [x] Partition manager (create, drop, maintain)
- [x] Dead letter table and management
- [x] Queue stats CLI
- [x] Migration script

### Phase 2: Alerter Integration
- [x] Move alerter design from docs/alerter-spec.md
- [x] Implement Discord webhook client
- [x] Error pattern classifier
- [x] Rate limiter / deduplication
- [x] Daily summary (scheduled at 6 AM)

### Phase 3: Log Analysis
- [ ] Docker log tailer
- [ ] Error search
- [ ] Log export

### Phase 4: Health & Monitoring
- [ ] Service health checker
- [ ] Prometheus metrics endpoint
- [ ] Grafana dashboard templates

## Related Documentation

- [Alerter Spec](../../docs/alerter-spec.md) - Discord alerting design
- [Postmark Spec](../../postmark/docs/project-spec.md) - Gmail sync service
- [Triage Spec](../../triage/docs/project-spec.md) - Classification service
- [Cortex Overview](../../docs/cortex-overview.md) - System architecture
