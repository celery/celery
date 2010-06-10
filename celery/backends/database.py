from datetime import datetime


from celery import conf
from celery.backends.base import BaseDictBackend
from celery.db.models import Task, TaskSet
from celery.db.session import ResultSession
from celery.exceptions import ImproperlyConfigured


class DatabaseBackend(BaseDictBackend):
    """The database result backend."""

    def __init__(self, dburi=conf.RESULT_DBURI,
            engine_options=None, **kwargs):
        if not dburi:
            raise ImproperlyConfigured(
                    "Missing connection string! Do you have "
                    "CELERY_RESULT_DBURI set to a real value?")
        self.dburi = dburi
        self.engine_options = dict(engine_options or {},
                                   **conf.RESULT_ENGINE_OPTIONS or {})
        super(DatabaseBackend, self).__init__(**kwargs)

    def ResultSession(self):
        return ResultSession(dburi=self.dburi, **self.engine_options)

    def _store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        session = self.ResultSession()
        try:
            tasks = session.query(Task).filter(Task.task_id == task_id).all()
            if not tasks:
                task = Task(task_id)
                session.add(task)
                session.flush()
            else:
                task = tasks[0]
            task.result = result
            task.status = status
            task.traceback = traceback
            session.commit()
        finally:
            session.close()
        return result

    def _save_taskset(self, taskset_id, result):
        """Store the result of an executed taskset."""
        taskset = TaskSet(taskset_id, result)
        session = self.ResultSession()
        try:
            session.add(taskset)
            session.flush()
            session.commit()
        finally:
            session.close()
        return result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        session = self.ResultSession()
        try:
            task = None
            for task in session.query(Task).filter(Task.task_id == task_id):
                break
            if not task:
                task = Task(task_id)
                session.add(task)
                session.flush()
                session.commit()
            if task:
                return task.to_dict()
        finally:
            session.close()

    def _restore_taskset(self, taskset_id):
        """Get taskset metadata for a taskset by id."""
        session = self.ResultSession()
        try:
            qs = session.query(TaskSet)
            for taskset in qs.filter(TaskSet.taskset_id == taskset_id):
                return taskset.to_dict()
        finally:
            session.close()

    def cleanup(self):
        """Delete expired metadata."""
        expires = conf.TASK_RESULT_EXPIRES
        session = self.ResultSession()
        try:
            for task in session.query(Task).filter(
                    Task.date_done < (datetime.now() - expires)):
                session.delete(task)
            for taskset in session.query(TaskSet).filter(
                    TaskSet.date_done < (datetime.now() - expires)):
                session.delete(taskset)
            session.commit()
        finally:
            session.close()
