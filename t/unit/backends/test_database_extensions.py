import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import Index, MetaData, String, Table
from sqlalchemy.types import JSON, PickleType

from celery.backends.database import DatabaseBackend
from celery.backends.database.extensions import JsonResultExtension, SchemaExtension


class ResultColumnToStringExtension(SchemaExtension):
    """Extension that changes the result column type to String."""

    def extend(self, table: Table, metadata: MetaData) -> None:
        if table.name == "celery_taskmeta":
            table.c.result.type = String(255)


class StatusIndexExtension(SchemaExtension):
    """Extension that adds a non-unique index on the status column."""

    def extend(self, table: Table, metadata: MetaData) -> None:
        if table.name == "celery_taskmeta":
            Index("idx_taskmeta_status", table.c.status, unique=False)


class GroupResultColumnToStringExtension(SchemaExtension):
    """Extension that changes the group result column type to String."""

    def extend(self, table: Table, metadata: MetaData) -> None:
        if table.name == "celery_tasksetmeta":
            table.c.result.type = String(255)


@pytest.mark.usefixtures("depends_on_current_app")
class test_SchemaExtensions:
    @pytest.fixture(autouse=True)
    def setup_db(self) -> None:
        self.uri: str = "sqlite:///:memory:"
        # Keep table creation enabled to make schema visible to inspection
        self.app.conf.database_create_tables_at_setup = True
        self.app.conf.database_table_schemas = {}
        self.app.conf.database_table_names = {}

    def test_no_extensions_does_not_modify_schema(self) -> None:
        # By default, schema_extensions=None should leave schema untouched
        backend = DatabaseBackend(
            self.uri,
            app=self.app,
            schema_extensions=None,  # explicit, but default anyway
        )
        assert isinstance(backend.task_cls.__table__.c.result.type, PickleType)
        assert isinstance(backend.taskset_cls.__table__.c.result.type, PickleType)

    def test_result_column_type_extension_applied(self) -> None:
        backend = DatabaseBackend(
            self.uri,
            app=self.app,
            schema_extensions={"task": [ResultColumnToStringExtension()]},
        )
        col_type = backend.task_cls.__table__.c.result.type
        assert isinstance(col_type, String), (
            "Expected result column type to be String after extension"
        )

    def test_status_index_extension_applied(self) -> None:
        backend = DatabaseBackend(
            self.uri,
            app=self.app,
            schema_extensions={"task": [StatusIndexExtension()]},
        )
        index_names = [ix.name for ix in backend.task_cls.__table__.indexes]
        assert "idx_taskmeta_status" in index_names, (
            "Expected idx_taskmeta_status index to be present"
        )

    def test_extensions_do_not_affect_group(self) -> None:
        backend = DatabaseBackend(
            self.uri,
            app=self.app,
            schema_extensions={"task": [ResultColumnToStringExtension()]},
        )
        col_type = backend.taskset_cls.__table__.c.result.type
        assert isinstance(col_type, PickleType), (
            "TaskSet result column should remain PickleType"
        )

    def test_group_extensions_applied(self) -> None:
        backend = DatabaseBackend(
            self.uri,
            app=self.app,
            schema_extensions={"group": [GroupResultColumnToStringExtension()]},
        )
        col_type = backend.taskset_cls.__table__.c.result.type
        assert isinstance(col_type, String), (
            "Expected TaskSet result column type to be String after extension"
        )

    def test_extensions_apply_to_extended_task(self) -> None:
        # Enable extended result so TaskExtended is used
        self.app.conf.result_extended = True
        backend = DatabaseBackend(
            self.uri,
            app=self.app,
            schema_extensions={"task": [ResultColumnToStringExtension()]},
        )
        assert backend.task_cls.__name__ == "TaskExtended"
        col_type = backend.task_cls.__table__.c.result.type
        assert isinstance(col_type, String), (
            "Expected TaskExtended result column type to be String after extension"
        )

    def test_json_result_extension_applied(self) -> None:
        backend = DatabaseBackend(
            self.uri,
            app=self.app,
            schema_extensions={"task": [JsonResultExtension()]},
        )
        col_type = backend.task_cls.__table__.c.result.type
        assert isinstance(col_type, JSON), (
            "Expected result column type to be JSON after JsonResultExtension"
        )

    def test_protected_column_warning(self) -> None:
        """Modifying a protected column should emit a warning."""

        class OverrideStatusExtension(SchemaExtension):
            def extend(self, table: Table, metadata: MetaData) -> None:
                if table.name == "celery_taskmeta":
                    table.c.status.type = String(255)

        import celery.backends.database.extensions as ext_mod
        with pytest.MonkeyPatch.context() as mp:
            warnings = []
            mp.setattr(ext_mod.logger, "warning", lambda *a, **kw: warnings.append(a))
            DatabaseBackend(
                self.uri,
                app=self.app,
                schema_extensions={"task": [OverrideStatusExtension()]},
            )
            assert any("protected column" in str(w) for w in warnings), (
                "Expected a warning about modifying a protected column"
            )

    def test_no_warning_for_safe_column(self) -> None:
        """Modifying 'result' (non-protected) should NOT emit a warning."""
        import celery.backends.database.extensions as ext_mod
        with pytest.MonkeyPatch.context() as mp:
            warnings = []
            mp.setattr(ext_mod.logger, "warning", lambda *a, **kw: warnings.append(a))
            DatabaseBackend(
                self.uri,
                app=self.app,
                schema_extensions={"task": [JsonResultExtension()]},
            )
            assert not warnings, (
                "No warning expected when modifying non-protected 'result' column"
            )
