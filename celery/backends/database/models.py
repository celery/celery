# -*- coding: utf-8 -*-
"""Database models used by the SQLAlchemy result store backend."""
from __future__ import absolute_import, unicode_literals

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.types import PickleType
from sqlalchemy.ext.declarative import declared_attr

from celery import states
from celery.five import python_2_unicode_compatible

from .session import ResultModelBase

__all__ = ('Task', 'TaskExtended', 'TaskSet')

class PickleXlType(PickleType):
    """PickleType with increased storage capacity on MySQL

    Uses MySQL's LONGBLOB column type, which can store up to 4GB per row.
    """

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            return dialect.type_descriptor(LONGBLOB())
        return super(PickleXlType, self).load_dialect_impl(dialect)


@python_2_unicode_compatible
class TaskBase(ResultModelBase):
    """Task result/status."""

    __tablename__ = 'celery_taskmeta'
    __table_args__ = {'sqlite_autoincrement': True}

    id = sa.Column(sa.Integer, sa.Sequence('task_id_sequence'),
                   primary_key=True, autoincrement=True)
    task_id = sa.Column(sa.String(155), unique=True)
    status = sa.Column(sa.String(50), default=states.PENDING)
    date_done = sa.Column(sa.DateTime, default=datetime.utcnow,
                          onupdate=datetime.utcnow, nullable=True)
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


class TaskExtendedBase(TaskBase):
    """For the extend result."""

    __tablename__ = 'celery_taskmeta'
    __table_args__ = {'sqlite_autoincrement': True, 'extend_existing': True}

    name = sa.Column(sa.String(155), nullable=True)
    args = sa.Column(sa.LargeBinary, nullable=True)
    kwargs = sa.Column(sa.LargeBinary, nullable=True)
    worker = sa.Column(sa.String(155), nullable=True)
    retries = sa.Column(sa.Integer, nullable=True)
    queue = sa.Column(sa.String(155), nullable=True)

    def to_dict(self):
        task_dict = super(TaskExtendedBase, self).to_dict()
        task_dict.update({
            'name': self.name,
            'args': self.args,
            'kwargs': self.kwargs,
            'worker': self.worker,
            'retries': self.retries,
            'queue': self.queue,
        })
        return task_dict


@python_2_unicode_compatible
class TaskSetBase(ResultModelBase):
    """TaskSet result."""

    __tablename__ = 'celery_tasksetmeta'
    __table_args__ = {'sqlite_autoincrement': True}

    id = sa.Column(sa.Integer, sa.Sequence('taskset_id_sequence'),
                   autoincrement=True, primary_key=True)
    taskset_id = sa.Column(sa.String(155), unique=True)
    date_done = sa.Column(sa.DateTime, default=datetime.utcnow,
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
        return '<TaskSet: {0.taskset_id}>'.format(self)


def task_storage_factory(large_results, extended_results):
    result_column_class = PickleXlType if large_results else PickleType
    task_base_class = TaskExtendedBase if extended_results else TaskBase

    task_class = type(
        str('Task'),
        (task_base_class,),
        {
            'result': sa.Column(result_column_class, nullable=True)
        }
    )
    task_set_class = type(
        str('TaskSet'),
        (TaskSetBase,),
        {
            'result': sa.Column(result_column_class, nullable=True)
        }
    )

    return task_class, task_set_class
