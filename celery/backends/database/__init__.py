# -*- coding: utf-8 -*-
"""
    celery.backends.database
    ~~~~~~~~~~~~~~~~~~~~~~~~

    SQLAlchemy result store backend.

"""
from __future__ import absolute_import

from functools import wraps

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.utils.timeutils import maybe_timedelta

from celery.backends.base import BaseDictBackend

from .models import Task, TaskSet
from .session import ResultSession


def _sqlalchemy_installed():
    try:
        import sqlalchemy
    except ImportError:
        raise ImproperlyConfigured(
            'The database result backend requires SQLAlchemy to be installed.'
            'See http://pypi.python.org/pypi/SQLAlchemy')
    return sqlalchemy
_sqlalchemy_installed()

from sqlalchemy.exc import DatabaseError, OperationalError


def retry(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        max_retries = kwargs.pop('max_retries', 3)

        for retries in xrange(max_retries + 1):
            try:
                return fun(*args, **kwargs)
            except (DatabaseError, OperationalError):
                if retries + 1 > max_retries:
                    raise

    return _inner


class DatabaseBackend(BaseDictBackend):
    """The database result backend."""
    # ResultSet.iterate should sleep this much between each pool,
    # to not bombard the database with queries.
    subpolling_interval = 0.5

    def __init__(self, dburi=None, expires=None,
                 engine_options=None, **kwargs):
        super(DatabaseBackend, self).__init__(**kwargs)
        conf = self.app.conf
        self.expires = maybe_timedelta(self.prepare_expires(expires))
        self.dburi = dburi or conf.CELERY_RESULT_DBURI
        self.engine_options = dict(
            engine_options or {},
            **conf.CELERY_RESULT_ENGINE_OPTIONS or {})
        self.short_lived_sessions = kwargs.get(
            'short_lived_sessions',
            conf.CELERY_RESULT_DB_SHORT_LIVED_SESSIONS,
        )
        if not self.dburi:
            raise ImproperlyConfigured(
                'Missing connection string! Do you have '
                'CELERY_RESULT_DBURI set to a real value?')

    def ResultSession(self):
        return ResultSession(
            dburi=self.dburi,
            short_lived_sessions=self.short_lived_sessions,
            **self.engine_options
        )

    @retry
    def _store_result(self, task_id, result, status,
                      traceback=None, max_retries=3):
        """Store return value and status of an executed task."""
        session = self.ResultSession()
        try:
            task = session.query(Task).filter(Task.task_id == task_id).first()
            if not task:
                task = Task(task_id)
                session.add(task)
                session.flush()
            task.result = result
            task.status = status
            task.traceback = traceback
            session.commit()
            return result
        finally:
            session.close()

    @retry
    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        session = self.ResultSession()
        try:
            task = session.query(Task).filter(Task.task_id == task_id).first()
            if task is None:
                task = Task(task_id)
                task.status = states.PENDING
                task.result = None
            return task.to_dict()
        finally:
            session.close()

    @retry
    def _save_group(self, group_id, result):
        """Store the result of an executed group."""
        session = self.ResultSession()
        try:
            group = TaskSet(group_id, result)
            session.add(group)
            session.flush()
            session.commit()
            return result
        finally:
            session.close()

    @retry
    def _restore_group(self, group_id):
        """Get metadata for group by id."""
        session = self.ResultSession()
        try:
            group = session.query(TaskSet).filter(
                TaskSet.taskset_id == group_id).first()
            if group:
                return group.to_dict()
        finally:
            session.close()

    @retry
    def _delete_group(self, group_id):
        """Delete metadata for group by id."""
        session = self.ResultSession()
        try:
            session.query(TaskSet).filter(
                TaskSet.taskset_id == group_id).delete()
            session.flush()
            session.commit()
        finally:
            session.close()

    @retry
    def _forget(self, task_id):
        """Forget about result."""
        session = self.ResultSession()
        try:
            session.query(Task).filter(Task.task_id == task_id).delete()
            session.commit()
        finally:
            session.close()

    def cleanup(self):
        """Delete expired metadata."""
        session = self.ResultSession()
        expires = self.expires
        now = self.app.now()
        try:
            session.query(Task).filter(
                Task.date_done < (now - expires)).delete()
            session.query(TaskSet).filter(
                TaskSet.date_done < (now - expires)).delete()
            session.commit()
        finally:
            session.close()

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(
            dict(dburi=self.dburi,
                 expires=self.expires,
                 engine_options=self.engine_options))
        return super(DatabaseBackend, self).__reduce__(args, kwargs)
