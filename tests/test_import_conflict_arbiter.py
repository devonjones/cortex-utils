"""The upsert arbiter in importer.py must exist in the migrations.

This is the regression guard for cortex-apd6. `import_yaml_to_db` upserted
email mappings with `ON CONFLICT ON CONSTRAINT unique_email_mapping`. That
constraint is created by postmark migration 002 on the OLD config-versioned
table over three columns, and migration 003 explicitly DROPs it while building
the replacement table -- which gets only a partial unique index instead.

So nothing in the repo ever created the constraint the code named. Every config
import worked solely because a production database had it hand-added, out of
band. `PUT /config` on a database built from these migrations failed outright,
and the divergence was invisible until someone tried it.

Naming a database object in SQL is a promise that a migration creates it. These
tests check the promise statically -- no database required -- so the build fails
instead of a deploy.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

# Locate importer.py by IMPORTING it, not by guessing a path. The previous
# version did `parents[2] / "utils" / "src" / ...`, which assumed the checkout
# directory is named "utils" -- true in Devon's multi-repo tree, false in CI,
# where actions/checkout names it "cortex-utils". The result was three tests
# failing with FileNotFoundError rather than asserting anything.
#
# Exactly the disease this file was rewritten to cure one commit earlier: a
# test coupled to one machine's directory layout. The module object knows
# where it lives; nothing else has to.
import cortex_utils.triage_config.importer as _importer_module

IMPORTER = Path(_importer_module.__file__)

# The sibling postmark checkout genuinely only exists in the multi-repo tree,
# so this one IS a guess -- and _migration_sql() returns None when it misses,
# which the cross-repo tests below skip on. parents[1] is this repo's root
# whatever it is called.
MIGRATIONS = Path(__file__).resolve().parents[1].parent / "postmark" / "migrations"


def _importer_sql() -> str:
    """importer.py with `#` comments stripped.

    The fix for cortex-apd6 documents the old broken clause verbatim in a
    comment, so a naive substring search over the raw file matches the prose
    explaining the bug rather than any executable SQL. Strip comments and
    assert against code only.
    """
    return "\n".join(
        re.sub(r"#.*$", "", line) for line in IMPORTER.read_text(encoding="utf-8").splitlines()
    )


def _migration_sql() -> str | None:
    """Sibling postmark migrations, or None when they aren't checked out.

    cortex-utils CI does a single `actions/checkout@v4` of THIS repo, so
    `../postmark/migrations` exists only in Devon's multi-repo working copy.
    The cross-repo tests below therefore cannot run in CI -- which is exactly
    why the load-bearing assertion (`test_mapping_upsert_targets_the_partial_
    unique_index`) deliberately does NOT depend on them. It reads importer.py
    alone and always runs.
    """
    if not MIGRATIONS.is_dir():
        return None
    return "\n".join(p.read_text(encoding="utf-8") for p in sorted(MIGRATIONS.glob("*.sql")))


def test_no_upsert_names_a_constraint_the_migrations_do_not_create() -> None:
    """Any `ON CONFLICT ON CONSTRAINT <name>` must be created by a migration.

    Prefer inferring the arbiter by column list + predicate. A partial unique
    index -- which is what this schema uses -- has no constraint name to cite,
    so naming one is a sign the code is describing a table that no longer
    exists.
    """
    named = re.findall(r"ON\s+CONFLICT\s+ON\s+CONSTRAINT\s+(\w+)", _importer_sql(), re.IGNORECASE)
    if not named:
        return  # nothing named at all -- the state this PR establishes

    migrations = _migration_sql()
    if migrations is None:
        pytest.skip("sibling postmark/migrations not checked out (expected in CI)")
    missing = [
        name
        for name in named
        if not re.search(rf"CONSTRAINT\s+{re.escape(name)}\s+UNIQUE", migrations, re.I)
        and not re.search(rf"ADD\s+CONSTRAINT\s+{re.escape(name)}\b", migrations, re.I)
    ]
    assert not missing, (
        f"importer.py names constraint(s) {missing} that no migration creates. "
        "Either add the migration or infer the arbiter by columns + predicate."
    )


def test_mapping_upsert_targets_the_partial_unique_index() -> None:
    """The arbiter must match idx_email_mappings_active from migration 003.

    That index is UNIQUE (mapping_type, email_address) WHERE deleted_at IS NULL.
    The predicate is the load-bearing part: uniqueness holds over LIVE rows
    only, so a soft-deleted mapping can be recreated. An unconditional arbiter
    would wrongly block that.
    """
    sql = _importer_sql()
    assert "ON CONFLICT ON CONSTRAINT unique_email_mapping" not in sql, (
        "unique_email_mapping does not exist on triage_email_mappings; "
        "migration 003 drops it. See cortex-apd6."
    )
    assert re.search(
        r"ON\s+CONFLICT\s*\(\s*mapping_type\s*,\s*email_address\s*\)\s*"
        r"WHERE\s+deleted_at\s+IS\s+NULL",
        sql,
        re.IGNORECASE,
    ), (
        "mapping upsert must infer the partial unique index: "
        "ON CONFLICT (mapping_type, email_address) WHERE deleted_at IS NULL"
    )


def test_migrations_still_define_that_partial_index() -> None:
    """Guard the other direction: don't drop the index the importer relies on.

    Cross-repo, so it skips in CI. `test_upsert_arbiter_matches_the_vendored_
    index_shape` below covers the same ground without the sibling checkout.
    """
    migrations = _migration_sql()
    if migrations is None:
        pytest.skip("sibling postmark/migrations not checked out (expected in CI)")
    assert re.search(
        r"CREATE\s+UNIQUE\s+INDEX[^;]*?ON\s+triage_email_mappings\w*\s*"
        r"\(\s*mapping_type\s*,\s*email_address\s*\)[^;]*?"
        r"WHERE\s+deleted_at\s+IS\s+NULL",
        migrations,
        re.IGNORECASE | re.DOTALL,
    ), (
        "no migration creates the partial unique index on "
        "(mapping_type, email_address) WHERE deleted_at IS NULL"
    )


# The expected arbiter, vendored so CI enforces it without a sibling checkout.
# Must stay in sync with postmark migration 003 -- which RENAMES
# idx_email_mappings_unique_active to this name rather than creating it -- and
# 004, which drops the stray named constraint that used to shadow it and
# re-creates this index idempotently.
#
# COLUMNS and PREDICATE are what the upsert actually infers, and are asserted
# against importer.py below with no checkout required. INDEX is the name, which
# Postgres does not use for inference at all -- it is checked against the
# migrations by test_the_migrations_create_the_arbiter_under_the_expected_name,
# which therefore skips in CI.
EXPECTED_INDEX = "idx_email_mappings_active"
EXPECTED_COLUMNS = ("mapping_type", "email_address")
EXPECTED_PREDICATE = "deleted_at IS NULL"


def test_upsert_arbiter_matches_the_vendored_index_shape() -> None:
    """Runs everywhere, including CI. This is the one that actually guards.

    The two cross-repo tests above skip without a postmark checkout, so if the
    invariant lived only there, cortex-apd6 could recur with CI green -- the
    exact failure mode this file exists to prevent.
    """
    sql = _importer_sql()
    match = re.search(
        r"ON\s+CONFLICT\s*\(([^)]*)\)\s*WHERE\s+([^\n]+?)\s*$",
        sql,
        re.IGNORECASE | re.MULTILINE,
    )
    assert match, "mapping upsert must infer an arbiter by columns + predicate"

    columns = tuple(c.strip() for c in match.group(1).split(","))
    assert columns == EXPECTED_COLUMNS, (
        f"arbiter columns {columns} != {EXPECTED_COLUMNS} from migration 003"
    )
    assert EXPECTED_PREDICATE.lower() in match.group(2).lower(), (
        f"arbiter predicate must be '{EXPECTED_PREDICATE}' -- uniqueness holds "
        "over live rows only, so a soft-deleted mapping can be recreated"
    )


def test_the_migrations_create_the_arbiter_under_the_expected_name() -> None:
    """EXPECTED_INDEX must name an index some migration actually establishes.

    Round 4 review: EXPECTED_INDEX was never read, while the comment above it
    promised a sync check against migrations 003 and 004. The shape assertions
    match on columns and predicate, which is the right way to check an
    arbiter -- Postgres infers a partial unique index by shape, not by name --
    but it leaves the name unverified, so the constant could drift from the
    schema and nothing would notice.

    Cross-repo, so it skips in CI like its two siblings. The name is only
    load-bearing for humans reading migration 004's assertions and for
    `CREATE UNIQUE INDEX IF NOT EXISTS`, which no-ops on a same-named index;
    the shape tests remain the ones that guard the upsert itself.
    """
    migrations = _migration_sql()
    if migrations is None:
        pytest.skip("sibling postmark/migrations not checked out (expected in CI)")

    # CREATE or RENAME TO. Migration 003 does not create this name -- it
    # renames idx_email_mappings_unique_active to it (003:178). Only 004's
    # idempotent re-create matches the CREATE form, so a CREATE-only assertion
    # would silently start depending on 004 rather than on the index existing.
    assert re.search(
        rf"(CREATE\s+UNIQUE\s+INDEX(\s+IF\s+NOT\s+EXISTS)?\s+{re.escape(EXPECTED_INDEX)}\b"
        rf"|RENAME\s+TO\s+{re.escape(EXPECTED_INDEX)}\b)",
        migrations,
        re.IGNORECASE,
    ), f"no migration creates or renames an index to {EXPECTED_INDEX}"

    columns = r"\s*,\s*".join(re.escape(c) for c in EXPECTED_COLUMNS)
    assert re.search(
        rf"CREATE\s+UNIQUE\s+INDEX(\s+IF\s+NOT\s+EXISTS)?\s+{re.escape(EXPECTED_INDEX)}\b"
        rf"[^;]*?\(\s*{columns}\s*\)[^;]*?WHERE\s+{re.escape(EXPECTED_PREDICATE)}",
        migrations,
        re.IGNORECASE | re.DOTALL,
    ), (
        f"{EXPECTED_INDEX} exists in the migrations but not with the shape the "
        f"importer infers: ({', '.join(EXPECTED_COLUMNS)}) WHERE {EXPECTED_PREDICATE}"
    )
