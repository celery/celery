# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Celery is a distributed task queue for Python (v5.6.x, codename "recovery"). It supports multiple message brokers (RabbitMQ, Redis, SQS, etc.) and result backends (Redis, database, Elasticsearch, etc.). Built on top of **kombu** (messaging library) and **billiard** (multiprocessing fork).

## Common Commands

### Testing
```bash
# Run all unit tests
tox -e 3.13-unit

# Run a specific test by keyword
tox -e 3.13-unit -- -k "test_name_pattern"

# Run a specific test file
tox -e 3.13-unit -- t/unit/app/test_base.py

# Run tests directly with pytest (faster iteration, skips coverage; requires tox venv)
.tox/3.13-unit/bin/pytest t/unit/app/test_base.py -x -k "test_name"

# Run integration tests (requires local RabbitMQ + Redis)
tox -e 3.13-integration-rabbitmq_redis

# Run smoke tests
tox -e 3.13-smoke -- -n auto
```

### Linting
```bash
# Run all linters (pre-commit: flake8, isort, pyupgrade, codespell)
tox -e lint

# Run pre-commit on specific files
tox -e lint -- run --files celery/app/base.py
```

### Type Checking
```bash
tox -e mypy
```

### Documentation
```bash
make docs
# or via Docker:
make docker-docs
```

## Architecture

### Core Components

- **`celery/app/`** — The `Celery` application class (`base.py`). Central hub that holds config (`defaults.py`), task registry (`registry.py`), and AMQP integration (`amqp.py`). Annotations (`annotations.py`) allow modifying task attributes at runtime. `trace.py` handles task execution tracing within the worker.

- **`celery/worker/`** — Worker process implementation. `worker.py` is the main worker class. Uses the **bootsteps** pattern (`celery/bootsteps.py`) — a DAG of reusable startup/shutdown components (Blueprint/Step/StartStopStep). The `consumer/` subdirectory handles message consumption, heartbeats, gossip (worker discovery), and event dispatching.

- **`celery/backends/`** — Result backend implementations. `base.py` defines the base interface. Each file is a backend (redis, database, elasticsearch, cache, s3, etc.). Backends store task results and support operations like `AsyncResult.get()`.

- **`celery/canvas.py`** — Workflow primitives: `signature`, `chain`, `group`, `chord`, `chunks`, `xmap`, `xstarmap`. These compose tasks into complex workflows. Import from `celery` directly, not from this module.

- **`celery/bin/`** — CLI commands (built on `click`). Entry point: `celery/__main__.py`. Subcommands: worker, beat, inspect, control, etc.

- **`celery/beat.py`** — Periodic task scheduler. `celery/apps/beat.py` is the beat application.

- **`celery/concurrency/`** — Execution pool implementations (prefork via billiard, threads, eventlet, gevent).

- **`celery/signals.py`** — Signal definitions (task_prerun, task_postrun, worker_ready, etc.) used throughout for extensibility. Based on `blinker`-style dispatch.

- **`celery/contrib/`** — Utilities: `pytest.py` provides the pytest plugin and fixtures (`celery_app`, `celery_worker`), `django/` has Django integration, `abortable.py` supports task abortion.

### Key Patterns

- **Bootsteps**: Worker and consumer startup is organized as a dependency graph of steps. Each step implements `start()`/`stop()`/`terminate()`. See `celery/bootsteps.py` and `celery/worker/components.py`.

- **Lazy proxy imports**: `celery/__init__.py` uses `PromiseProxy` for lazy loading of `Celery`, `Task`, canvas primitives, etc. Actual imports resolve on first access.

- **App-based architecture**: Almost everything hangs off the `Celery` app instance. Tasks are bound to apps. Configuration is accessed via `app.conf`. The default app is accessible via `celery._state.get_current_app()`.

### Test Structure

Tests live in `t/` (not `tests/`):
- `t/unit/` — Unit tests (mirrors `celery/` structure: `t/unit/app/`, `t/unit/backends/`, etc.)
- `t/integration/` — Integration tests (require running brokers/backends)
- `t/smoke/` — Smoke tests

pytest config is in `pyproject.toml`. Test classes are named `test_*` (not `Test*`). Markers include `flaky`, `timeout`, `sleepdeprived_patched_module`, `masked_modules`.

### Configuration

- Settings are defined in `celery/app/defaults.py` with `Option` objects specifying type and default.
- Old setting names (pre-4.0 uppercase like `CELERY_TASK_ALWAYS_EAGER`) map to new lowercase names (like `task_always_eager`) via `_old_key_to_new`/`_new_key_to_old` in `celery/app/utils.py`.

## Style & Conventions

- Max line length: 117 characters (flake8)
- Import sorting: isort
- Python 3.10+ required
- Pre-commit hooks enforce: pyupgrade, flake8, isort, codespell, yesqa
- For user-facing behavior changes, add `.. versionchanged::` / `.. versionadded::` Sphinx directives in docs
