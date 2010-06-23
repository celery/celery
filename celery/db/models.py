from datetime import datetime

import sqlalchemy as sa

from celery import states
from celery.db.session import ResultModelBase
# See docstring of a805d4bd for an explanation for this workaround ;)
from celery.db.a805d4bd import PickleType


class Task(ResultModelBase):
    """Task result/status."""
    __tablename__ = "celery_taskmeta"
    __table_args__ = {"sqlite_autoincrement": True}

    id = sa.Column(sa.Integer, sa.Sequence("task_id_sequence"),
                   primary_key=True,
                   autoincrement=True)
    task_id = sa.Column(sa.String(255))
    status = sa.Column(sa.String(50), default=states.PENDING)
    result = sa.Column(PickleType, nullable=True)
    date_done = sa.Column(sa.DateTime, default=datetime.now,
                       onupdate=datetime.now, nullable=True)
    traceback = sa.Column(sa.Text, nullable=True)

    def __init__(self, task_id):
        self.task_id = task_id

    def __str__(self):
        return "<Task(%s, %s, %s, %s)>" % (self.task_id,
                                           self.result,
                                           self.status,
                                           self.traceback)

    def to_dict(self):
        return {"task_id": self.task_id,
                "status": self.status,
                "result": self.result,
                "date_done": self.date_done,
                "traceback": self.traceback}

    def __unicode__(self):
        return u"<Task: %s successful: %s>" % (self.task_id, self.status)


class TaskSet(ResultModelBase):
    """TaskSet result"""
    __tablename__ = "celery_tasksetmeta"
    __table_args__ = {"sqlite_autoincrement": True}

    id = sa.Column(sa.Integer, sa.Sequence("taskset_id_sequence"),
                autoincrement=True, primary_key=True)
    taskset_id = sa.Column(sa.String(255))
    result = sa.Column(sa.PickleType, nullable=True)
    date_done = sa.Column(sa.DateTime, default=datetime.now,
                       nullable=True)

    def __init__(self, task_id):
        self.task_id = task_id

    def __str__(self):
        return "<TaskSet(%s, %s)>" % (self.task_id, self.result)

    def to_dict(self):
        return {"taskset_id": self.taskset_id,
                "result": self.result,
                "date_done": self.date_done}

    def __unicode__(self):
        return u"<TaskSet: %s>" % (self.taskset_id)
