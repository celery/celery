import urllib
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from celery import conf
from celery.db.models import Task, TaskSet
from celery.backends.base import BaseDictBackend

server = '<sql server host>'
database = '<your db>'
userid = '<your user>'
password = '<your password>'
port = 1433
raw_cs = "DRIVER={FreeTDS};SERVER=%s;PORT=%d;DATABASE=%s;UID=%s;PWD=%s;CHARSET=UTF8;TDS_VERSION=8.0;TEXTSIZE=10000" % (server, port, database, userid, password)
#connection_string = "mssql:///?odbc_connect=%s" % urllib.quote_plus(raw_cs)
#connection_string = 'sqlite:////mnt/winctmp/celery.db'
connection_string = 'sqlite:///celery.db'
engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)


class DatabaseBackend(BaseDictBackend):
    """The database result backend."""

    def _store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        session = Session()
        try:
            tasks = session.query(Task).filter(Task.task_id == task_id).all()
            if not tasks:
                task = Task(task_id)
                session.add(task)
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
        session = Session()
        try:
            session.add(taskset)
            session.commit()
        finally:
            session.close()
        return result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        session = Session()
        try:
            task = None
            for task in session.query(Task).filter(Task.task_id == task_id):
                break
            if not task:
                task = Task(task_id)
                session.add(task)
                session.commit()
            if task:
                return task.to_dict()
        finally:
            session.close()

    def _restore_taskset(self, taskset_id):
        """Get taskset metadata for a taskset by id."""
        session = Session()
        try:
            qs = session.query(TaskSet)
            for taskset in qs.filter(TaskSet.task_id == task_id):
                return taskset.to_dict()
        finally:
            session.close()

    def cleanup(self):
        """Delete expired metadata."""
        expires = conf.TASK_RESULT_EXPIRES
        session = Session()
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
