"""Schema extensions for Celery result backend tables."""

from abc import ABC, abstractmethod

from sqlalchemy import JSON, MetaData, Table


class SchemaExtension(ABC):
    """Base class for schema extensions.

    A schema extension can modify the SQLAlchemy Table definition
    (e.g., change column types, add indexes, or constraints).
    """

    @abstractmethod
    def extend(self, table: Table, metadata: MetaData) -> None:
        """Extend or modify the given table in place."""
        ...  # pragma: no cover


class JsonResultExtension(SchemaExtension):
    def extend(self, table: Table, metadata: MetaData) -> None:
        table.c.result.type = JSON()
