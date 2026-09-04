"""Schema extensions for Celery result backend tables.

Extensions allow applications to customize the schema of Celery's
SQLAlchemy result backend tables (e.g., change column types, add indexes)
without monkey-patching.

.. note::

    If you use schema extensions, you are responsible for managing your own
    database migrations (e.g., via Alembic).  Celery's default
    ``metadata.create_all()`` is additive — it will create missing tables
    and columns but will **not** alter or drop existing ones.  Future Celery
    releases may add new core columns; a migration tool gives you full
    control over how those changes are applied alongside your extensions.

"""

import logging
from abc import ABC, abstractmethod

from sqlalchemy import JSON, MetaData, Table

logger = logging.getLogger(__name__)

# Columns that are essential to the result backend's internal operation.
# Extensions that modify these risk breaking core Celery functionality.
TASK_PROTECTED_COLUMNS = frozenset({'id', 'task_id', 'status', 'date_done'})
GROUP_PROTECTED_COLUMNS = frozenset({'id', 'taskset_id', 'date_done'})

PROTECTED_COLUMNS = {
    'celery_taskmeta': TASK_PROTECTED_COLUMNS,
    'celery_tasksetmeta': GROUP_PROTECTED_COLUMNS,
}


class SchemaExtension(ABC):
    """Base class for schema extensions.

    A schema extension can modify the SQLAlchemy Table definition
    (e.g., change column types, add indexes, or constraints).

    Subclasses must implement :meth:`extend`.  Extensions are applied
    once during model configuration, before tables are created.

    Example::

        class StatusIndexExtension(SchemaExtension):
            def extend(self, table, metadata):
                if table.name == "celery_taskmeta":
                    Index("idx_taskmeta_status", table.c.status)
    """

    @abstractmethod
    def extend(self, table: Table, metadata: MetaData) -> None:
        """Extend or modify the given table in place."""
        ...  # pragma: no cover


class JsonResultExtension(SchemaExtension):
    """Replace the ``result`` column's type with ``JSON``.

    Useful on PostgreSQL and other dialects that support native JSON
    storage, avoiding the overhead and opacity of ``PickleType``.

    Example::

        backend = DatabaseBackend(
            url="postgresql://...",
            app=app,
            schema_extensions={"task": [JsonResultExtension()]},
        )
    """

    def extend(self, table: Table, metadata: MetaData) -> None:
        table.c.result.type = JSON()


def _warn_if_protected(table: Table, original_types: dict) -> None:
    """Emit a warning if an extension modified a protected column's type."""
    protected = PROTECTED_COLUMNS.get(table.name, frozenset())
    for col_name in protected:
        col = table.c.get(col_name)
        if col is not None and col_name in original_types:
            before, after = repr(original_types[col_name]), repr(col.type)
            if before != after:
                logger.warning(
                    "Schema extension modified protected column '%s.%s' "
                    "(type changed from %s to %s). This may break core "
                    "Celery functionality.",
                    table.name, col_name, before, after,
                )
