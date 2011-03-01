from datetime import datetime

from celery import states
from celery.backends.base import BaseDictBackend
from celery.db.models import Task, TaskSet
from celery.db.session import ResultSession
from celery.exceptions import ImproperlyConfigured
from celery.utils.timeutils import maybe_timedelta


def _sqlalchemy_installed():
    try:
        import sqlalchemy
    except ImportError:
        raise ImproperlyConfigured(
            "The database result backend requires SQLAlchemy to be installed."
            "See http://pypi.python.org/pypi/SQLAlchemy")
    return sqlalchemy
_sqlalchemy_installed()


class DatabaseBackend(BaseDictBackend):
    """The database result backend."""

    def __init__(self, dburi=None, result_expires=None,
            engine_options=None, **kwargs):
        super(DatabaseBackend, self).__init__(**kwargs)
        self.result_expires = result_expires or \
                                maybe_timedelta(
                                    self.app.conf.CELERY_TASK_RESULT_EXPIRES)
        self.dburi = dburi or self.app.conf.CELERY_RESULT_DBURI
        self.engine_options = dict(engine_options or {},
                        **self.app.conf.CELERY_RESULT_ENGINE_OPTIONS or {})
        if not self.dburi:
            raise ImproperlyConfigured(
                    "Missing connection string! Do you have "
                    "CELERY_RESULT_DBURI set to a real value?")

    def ResultSession(self):
        return ResultSession(dburi=self.dburi, **self.engine_options)

    def _store_result(self, task_id, result, status, traceback=None):
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
        finally:
            session.close()
        return result

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

    def _save_taskset(self, taskset_id, result):
        """Store the result of an executed taskset."""
        session = self.ResultSession()
        try:
            taskset = TaskSet(taskset_id, result)
            session.add(taskset)
            session.flush()
            session.commit()
            return result
        finally:
            session.close()

    def _restore_taskset(self, taskset_id):
        """Get taskset metadata for a taskset by id."""
        session = self.ResultSession()
        try:
            taskset = session.query(TaskSet).filter(
                    TaskSet.taskset_id == taskset_id).first()
            if taskset:
                return taskset.to_dict()
        finally:
            session.close()

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
        expires = self.result_expires
        try:
            session.query(Task).filter(
                    Task.date_done < (datetime.now() - expires)).delete()
            session.query(TaskSet).filter(
                    TaskSet.date_done < (datetime.now() - expires)).delete()
            session.commit()
        finally:
            session.close()
