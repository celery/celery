# -*- coding: utf-8 -*-
"""Database models used by the SQLAlchemy result store backend."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
import sqlalchemy as sa
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.types import PickleType

from celery import states
<<<<<<< HEAD
from celery.five import python_2_unicode_compatible

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from .session import ResultModelBase

__all__ = ('Task', 'TaskSet')


class Task(ResultModelBase):
    """Task result/status."""

    __tablename__ = 'celery_taskmeta'
    __table_args__ = {'sqlite_autoincrement': True}

    id = sa.Column(sa.Integer, sa.Sequence('task_id_sequence'),
                   primary_key=True, autoincrement=True)
    task_id = sa.Column(sa.String(155), unique=True)
    status = sa.Column(sa.String(50), default=states.PENDING)
    result = sa.Column(PickleType, nullable=True)
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


class TaskSet(ResultModelBase):
    """TaskSet result."""

    __tablename__ = 'celery_tasksetmeta'
    __table_args__ = {'sqlite_autoincrement': True}

    id = sa.Column(sa.Integer, sa.Sequence('taskset_id_sequence'),
                   autoincrement=True, primary_key=True)
    taskset_id = sa.Column(sa.String(155), unique=True)
    result = sa.Column(PickleType, nullable=True)
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
