# Contributing to mylox

Thanks for taking a look! This is a small, single-maintainer project, so the
process is deliberately light.

## Getting started

```bash
git clone https://github.com/dainan13/mylox.git
cd mylox
python -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"      # runtime + pytest + ruff + build + twine
```

Requires Python 3.10+. No MySQL or PostgreSQL is needed for the unit tests: they
drive the whole engine with fake binlog events (`test/fakes.py`).

## Before opening a pull request

```bash
ruff check                                                  # must be clean
pytest test -q --ignore=test/test_integration.py            # must be green
```

If you touched decoding, DDL interpretation, checkpointing or GTID logic, please
add a unit test. If your change can only be proven against a real server, say so
in the PR description instead of skipping the test silently — the integration
suite (`test/test_integration.py`) needs a MySQL master/replica pair and a
PostgreSQL database, see [docs/testing.md](docs/testing.md).

## What goes where

| Path | Purpose |
| --- | --- |
| `src/mylox/schema/` | schema model, timeline, sqlglot DDL interpretation, `information_schema` loader |
| `src/mylox/engine/` | the binlog loop, pipeline state, handler API |
| `src/mylox/state/` | PostgreSQL checkpoints and schema history |
| `src/mylox/utils/`, `gtid.py`, `filtering.py`, `config.py` | self-contained helpers, each covered by its own test module |
| `docs/` | user and developer documentation (not shipped in the wheel) |
| `example/` | runnable example, covered by `test/test_example.py` |

Pure logic lives in small functions so it can be tested without a database;
anything that talks to MySQL/PostgreSQL goes through an injectable
`store` / `loader` / `stream_factory` so the tests can replace it.

## Code style

- `ruff check` with `line-length = 120`; imports sorted (`ruff check --fix`).
- `from __future__ import annotations` at the top of every module, type hints on
  public functions.
- Comments explain *why*, not *what*; the tricky invariants (checkpoint
  ordering, GTID intersections, schema at a position) are worth a comment.
- No new required dependency without a note in the PR.

## Commit messages

`area: short imperative summary` — for example
`engine: park the pipeline when a row cannot be decoded`.
Reference the affected module when it is not obvious.

## Releasing

Maintainers only; the steps are in
[docs/development.md](docs/development.md#5-发布流程github-actions--pypi).
In short: bump `pyproject.toml`, update `CHANGELOG.md`, push a `vX.Y.Z` tag, and
the `release` workflow publishes to PyPI through Trusted Publishing.

## License

By contributing you agree that your contribution is licensed under the MIT
license (see [LICENSE](LICENSE)).
