# -*- coding: utf-8 -*-
"""SQLAlchemy result store backend."""
from __future__ import absolute_import, unicode_literals

import logging

from contextlib import contextmanager

from vine.utils import wraps

from celery import states
from celery.backends.base import BaseBackend
from celery.exceptions import ImproperlyConfigured
from celery.five import range
from celery.utils.time import maybe_timedelta

from .models import Task
from .models import TaskSet
from .session import SessionManager

try:
    from sqlalchemy.exc import DatabaseError, InvalidRequestError
    from sqlalchemy.orm.exc import StaleDataError
except ImportError:  # pragma: no cover
    raise ImproperlyConfigured(
        'The database result backend requires SQLAlchemy to be installed.'
        'See https://pypi.org/project/SQLAlchemy/')

logger = logging.getLogger(__name__)

__all__ = ('DatabaseBackend',)


@contextmanager
def session_cleanup(session):
    try:
        yield
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def retry(fun):

    @wraps(fun)
    def _inner(*args, **kwargs):
        max_retries = kwargs.pop('max_retries', 3)

        for retries in range(max_retries):
            try:
                return fun(*args, **kwargs)
            except (DatabaseError, InvalidRequestError, StaleDataError):
                logger.warning(
                    'Failed operation %s.  Retrying %s more times.',
                    fun.__name__, max_retries - retries - 1,
                    exc_info=True)
                if retries + 1 >= max_retries:
                    raise

    return _inner


class DatabaseBackend(BaseBackend):
    """The database result backend."""

    # ResultSet.iterate should sleep this much between each pool,
    # to not bombard the database with queries.
    subpolling_interval = 0.5

    def __init__(self, dburi=None, engine_options=None, url=None, **kwargs):
        # The `url` argument was added later and is used by
        # the app to set backend by url (celery.app.backends.by_url)
        super(DatabaseBackend, self).__init__(
            expires_type=maybe_timedelta, url=url, **kwargs)
        conf = self.app.conf
        self.url = url or dburi or conf.database_url
        self.engine_options = dict(
            engine_options or {},
            **conf.database_engine_options or {})
        self.short_lived_sessions = kwargs.get(
            'short_lived_sessions',
            conf.database_short_lived_sessions)

        tablenames = conf.database_table_names or {}
        Task.__table__.name = tablenames.get('task', 'celery_taskmeta')
        TaskSet.__table__.name = tablenames.get('group', 'celery_tasksetmeta')

        if not self.url:
            raise ImproperlyConfigured(
                'Missing connection string! Do you have the'
                ' database_url setting set to a real value?')

    def ResultSession(self, session_manager=SessionManager()):
        return session_manager.session_factory(
            dburi=self.url,
            short_lived_sessions=self.short_lived_sessions,
            **self.engine_options)

    @retry
    def _store_result(self, task_id, result, state,
                      traceback=None, max_retries=3, **kwargs):
        """Store return value and state of an executed task."""
        session = self.ResultSession()
        with session_cleanup(session):
            task = list(session.query(Task).filter(Task.task_id == task_id))
            task = task and task[0]
            if not task:
                task = Task(task_id)
                session.add(task)
                session.flush()
            task.result = result
            task.status = state
            task.traceback = traceback
            session.commit()
            return result

    @retry
    def _get_task_meta_for(self, task_id):
        """Get task meta-data for a task by id."""
        session = self.ResultSession()
        with session_cleanup(session):
            task = list(session.query(Task).filter(Task.task_id == task_id))
            task = task and task[0]
            if not task:
                task = Task(task_id)
                task.status = states.PENDING
                task.result = None
            return self.meta_from_decoded(task.to_dict())

    @retry
    def _save_group(self, group_id, result):
        """Store the result of an executed group."""
        session = self.ResultSession()
        with session_cleanup(session):
            group = TaskSet(group_id, result)
            session.add(group)
            session.flush()
            session.commit()
            return result

    @retry
    def _restore_group(self, group_id):
        """Get meta-data for group by id."""
        session = self.ResultSession()
        with session_cleanup(session):
            group = session.query(TaskSet).filter(
                TaskSet.taskset_id == group_id).first()
            if group:
                return group.to_dict()

    @retry
    def _delete_group(self, group_id):
        """Delete meta-data for group by id."""
        session = self.ResultSession()
        with session_cleanup(session):
            session.query(TaskSet).filter(
                TaskSet.taskset_id == group_id).delete()
            session.flush()
            session.commit()

    @retry
    def _forget(self, task_id):
        """Forget about result."""
        session = self.ResultSession()
        with session_cleanup(session):
            session.query(Task).filter(Task.task_id == task_id).delete()
            session.commit()

    def cleanup(self):
        """Delete expired meta-data."""
        session = self.ResultSession()
        expires = self.expires
        now = self.app.now()
        with session_cleanup(session):
            session.query(Task).filter(
                Task.date_done < (now - expires)).delete()
            session.query(TaskSet).filter(
                TaskSet.date_done < (now - expires)).delete()
            session.commit()

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(
            {'dburi': self.url,
             'expires': self.expires,
             'engine_options': self.engine_options})
        return super(DatabaseBackend, self).__reduce__(args, kwargs)
