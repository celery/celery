"""Database models used by the SQLAlchemy result store backend."""
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.types import JSON as sa_JSON
from sqlalchemy.types import PickleType

from celery import states

from .session import ResultModelBase

__all__ = ('Task', 'TaskExtended', 'TaskSet',
           'TaskJSON', 'TaskExtendedJSON', 'TaskSetJSON',)

DialectSpecificInteger = sa.Integer().with_variant(sa.BigInteger, 'mssql')


class TaskBase(ResultModelBase):
    """Base class for task models."""
    __abstract__ = True

    id = sa.Column(DialectSpecificInteger, sa.Sequence('task_id_sequence'),
                   primary_key=True, autoincrement=True)
    task_id = sa.Column(sa.String(155), unique=True)
    status = sa.Column(sa.String(50), default=states.PENDING)
    date_done = sa.Column(sa.DateTime, default=datetime.now(timezone.utc),
                          onupdate=datetime.now(timezone.utc), nullable=True)
    traceback = sa.Column(sa.Text, nullable=True)

    def __init__(self, task_id):
        self.task_id = task_id

    def to_dict(self):
        return {
            'task_id': self.task_id,
            'status': self.status,
            'result': self.result,
            'traceback': self.traceback,
            'date_done': self.date_done,
        }

    def __repr__(self):
        return '<Task {0.task_id} state: {0.status}>'.format(self)

    @classmethod
    def configure(cls, schema=None, name=None):
        cls.__table__.schema = schema
        cls.id.default.schema = schema
        cls.__table__.name = name or cls.__tablename__


class TaskSetBase(ResultModelBase):
    """Base class for taskset models."""
    __abstract__ = True

    id = sa.Column(DialectSpecificInteger, sa.Sequence('taskset_id_sequence'),
                   autoincrement=True, primary_key=True)
    taskset_id = sa.Column(sa.String(155), unique=True)
    date_done = sa.Column(sa.DateTime, default=datetime.now(timezone.utc),
                          nullable=True)

    def __init__(self, taskset_id, result):
        self.taskset_id = taskset_id
        self.result = result

    def to_dict(self):
        return {
            'taskset_id': self.taskset_id,
            'result': self.result,
            'date_done': self.date_done,
        }

    def __repr__(self):
        return f'<TaskSet: {self.taskset_id}>'

    @classmethod
    def configure(cls, schema=None, name=None):
        cls.__table__.schema = schema
        cls.id.default.schema = schema
        cls.__table__.name = name or cls.__tablename__


class Task(TaskBase):
    """Task result/status."""

    __tablename__ = 'celery_taskmeta'
    __table_args__ = {'sqlite_autoincrement': True}

    result = sa.Column(PickleType, nullable=True)


class TaskJSON(TaskBase):
    """Task JSON result/status."""

    __tablename__ = 'celery_taskmeta_json'
    __table_args__ = {'sqlite_autoincrement': True}

    id = sa.Column(DialectSpecificInteger,
                   sa.Sequence('task_id_sequence_json'),
                   primary_key=True, autoincrement=True)
    result = sa.Column(sa_JSON, nullable=True)


class TaskExtended(TaskBase):
    """For the extend result."""

    __tablename__ = 'celery_taskmeta'
    __table_args__ = {'sqlite_autoincrement': True, 'extend_existing': True}

    name = sa.Column(sa.String(155), nullable=True)
    args = sa.Column(sa.LargeBinary, nullable=True)
    kwargs = sa.Column(sa.LargeBinary, nullable=True)
    worker = sa.Column(sa.String(155), nullable=True)
    retries = sa.Column(sa.Integer, nullable=True)
    queue = sa.Column(sa.String(155), nullable=True)

    result = sa.Column(PickleType, nullable=True)

    def to_dict(self):
        task_dict = super().to_dict()
        task_dict.update({
            'name': self.name,
            'args': self.args,
            'kwargs': self.kwargs,
            'worker': self.worker,
            'retries': self.retries,
            'queue': self.queue,
        })
        return task_dict


class TaskExtendedJSON(TaskBase):
    """Extended task with JSON result."""

    __tablename__ = 'celery_taskmeta_json'
    __table_args__ = {'sqlite_autoincrement': True, 'extend_existing': True}

    id = sa.Column(DialectSpecificInteger,
                   sa.Sequence('task_id_sequence_json'),
                   primary_key=True, autoincrement=True)
    result = sa.Column(sa_JSON, nullable=True)


class TaskSet(TaskSetBase):
    """TaskSet result."""
    __tablename__ = 'celery_tasksetmeta'
    __table_args__ = {'sqlite_autoincrement': True}

    result = sa.Column(PickleType, nullable=True)


class TaskSetJSON(TaskSetBase):
    """TaskSet JSON result."""

    __tablename__ = 'celery_tasksetmeta_json'
    __table_args__ = {'sqlite_autoincrement': True}

    id = sa.Column(DialectSpecificInteger,
                   sa.Sequence('taskset_id_sequence_json'),
                   autoincrement=True, primary_key=True)
    result = sa.Column(sa_JSON, nullable=True)
