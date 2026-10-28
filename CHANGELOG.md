# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0] - 2026-10-28

First public **beta** release (`Development Status :: 4 - Beta`). Verified
against MySQL 5.7.44 and MySQL 8.4.7 (each as one master plus two replicas) with
PostgreSQL 18.6: 178 unit tests and 9 integration tests pass on both MySQL
versions.

### Added

- **Shadow-free schema tracking.** The table schema is obtained from one
  `information_schema` snapshot at startup and then evolved locally by parsing
  the DDL statements carried by the binlog with sqlglot
  (`ddl_sqlglot`), falling back to a single `information_schema` query per
  affected table whenever a statement cannot be modelled with certainty
  (`ddl_fallback`). Every version is recorded in `mylox_schema_history`
  together with the binlog position it became effective at, so a lagging
  pipeline can resolve the schema of any position from the in-memory timeline.
- **Multi-pipeline engine.** One binlog stream fans out to any number of
  pipelines, each with its own checkpoint. `handle_row` is invoked once per
  pipeline inside a PostgreSQL SAVEPOINT, so a failing pipeline is parked
  without affecting the others.
- **Atomic checkpoints.** Business writes and checkpoint updates commit in the
  same PostgreSQL transaction; schema history uses a separate autocommit
  connection so a DDL can never commit a half-finished business transaction.
- **Real GTID bookkeeping.** `GtidSet` implements interval arithmetic, so the
  checkpoint stores the full executed set (not a single GTID) and a failover to
  another replica resumes with `auto_position` using the intersection of the
  pipelines' sets.
- **Catch-up and initialization.** Pending pipelines are initialized at a frozen
  replica anchor (`STOP REPLICA` -> snapshot -> `on_init` -> `START REPLICA`),
  with the anchor check driven by the last *handled* position.
- **Reconnection.** Transient failures are retried with exponential backoff and
  the checkpoints are reloaded from PostgreSQL; a source `server_uuid` change is
  detected on the reconnect path as well.
- **Typed settings.** `MySQLSettings`, `PostgresSettings`, `EngineConfig`, and
  `TableFilter` (both `.gitignore` and `include` rule dialects).
- `example/mirror_to_pg.py`: a runnable per-source-table mirror with a
  MySQL -> PostgreSQL type mapping, full load in `on_init`, and idempotent
  upserts/deletes.

### Fixed during the pre-release verification

- A `BLOB`/`BINARY` value holding non-UTF-8 bytes raised inside the binlog
  reader and made the engine reconnect-loop on the same event forever. The
  reader now runs with `ignore_decode_errors=True` and any undecodable event
  parks the affected pipelines instead of retrying indefinitely.
- `BIGINT UNSIGNED` (and friends) were decoded as signed whenever
  `binlog_row_metadata` was not `FULL` (the default, and always the case on
  MySQL 5.7), turning `18446744073709551615` into `-1`. Values are now repaired
  from the schema model.
- `stringify_bytes` no longer replaces undecodable bytes with plausible-looking
  text; such values keep their raw `bytes`.
- `SHOW MASTER STATUS` (removed in MySQL 8.4/8.5) and `STOP/START REPLICA`
  versus `STOP/START SLAVE` are handled by trying both spellings.
- A DDL seen outside a transaction now advances the checkpoint instead of
  re-applying itself after every restart.
- MySQL `SAVEPOINT` / `ROLLBACK TO SAVEPOINT` / `RELEASE SAVEPOINT` are mirrored
  onto the PostgreSQL transaction.
- Binlog file names are compared by their numeric suffix instead of as strings,
  and schema history is ordered by insertion id instead of `log_file`.
- Filter rules that match no table are reported at startup, because a rule
  written in the other dialect silently replicates nothing.

[Unreleased]: https://github.com/dainan13/mylox/compare/v0.8.0...HEAD
[0.8.0]: https://github.com/dainan13/mylox/releases/tag/v0.8.0
