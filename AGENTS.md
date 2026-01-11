# Repository Guidelines

## Project Structure & Module Organization
- `src/kalshi_bot/` contains all application code. Entry points live in `src/kalshi_bot/app/` (e.g., `collector.py`).
- `src/kalshi_bot/data/` owns SQLite helpers and migrations; `src/kalshi_bot/data/migrations/` holds ordered SQL files (`001_*.sql`, `002_*.sql`).
- `tests/` contains pytest tests (offline by default), using `test_*.py` naming.
- `scripts/` is reserved for utilities (currently empty).
- Local artifacts (e.g., `data/`, `logs/`, `.env`) are ignored by `.gitignore` and should not be committed.

## Build, Test, and Development Commands
- Create venv + install (dev tools included):
  - `python3.11 -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install -U pip`
  - `pip install -e ".[dev]"`
- Run the collector (creates DB, runs migrations, logs startup):
  - `python -m kalshi_bot.app.collector`
  - Coinbase only: `python -m kalshi_bot.app.collector --coinbase --seconds 30`
  - Debug stdout: add `--debug`
- Run tests:
  - `pytest`
- Optional lint/type checks (if dev deps installed):
  - `ruff check src tests`
  - `mypy src`

## Coding Style & Naming Conventions
- Python 3.11, 4-space indentation, and type hints where practical.
- Modules and functions: `snake_case`; classes: `PascalCase`.
- Keep logging structured JSON via `kalshi_bot.infra.logging`.
- Prefer small, explicit modules; avoid OS-specific paths.

## Testing Guidelines
- Framework: `pytest`.
- Naming: test files `test_*.py`, test functions `test_*`.
- Tests must not require network access. Use mock message sources for Coinbase ingestion.
- Schema tests should validate required tables/columns with `PRAGMA table_info`.

## Commit & Pull Request Guidelines
- Commit history conventions are not yet established in this repo; use concise, imperative subjects (e.g., "Add schema fields for opportunities").
- PRs should include: a short summary, tests run, and any schema or config changes. Link related issues if applicable.

## Security & Configuration Tips
- Keep secrets in `.env` (never commit). Required runtime values include `DB_PATH`, `LOG_PATH`, and `KALSHI_ENV`.
- Coinbase collector knobs: `COINBASE_WS_URL`, `COINBASE_PRODUCT_ID`, `COINBASE_STALE_SECONDS`, `COLLECTOR_SECONDS`.
- DB/log outputs (`data/`, `logs/`, `*.sqlite`) are local-only and ignored by git.
