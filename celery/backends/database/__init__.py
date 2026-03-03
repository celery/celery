"""SQLAlchemy result store backend."""
import logging
from contextlib import contextmanager

from celery import states
from celery.backends.base import BaseBackend
from celery.exceptions import ImproperlyConfigured
from celery.utils.time import maybe_timedelta

from .models import Task, TaskExtended, TaskSet
from .session import SessionManager

try:
    from sqlalchemy.exc import DatabaseError, InterfaceError, InvalidRequestError
    from sqlalchemy.orm.exc import StaleDataError
except ImportError:
    raise ImproperlyConfigured(
        'The database result backend requires SQLAlchemy to be installed.'
        'See https://pypi.org/project/SQLAlchemy/')

logger = logging.getLogger(__name__)

__all__ = ('DatabaseBackend',)

RETRYABLE_DB_ERRORS = (
    DatabaseError,
    InterfaceError,
    InvalidRequestError,
    StaleDataError,
)


@contextmanager
def session_cleanup(session):
    try:
        yield
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


class DatabaseBackend(BaseBackend):
    """The database result backend."""

    # ResultSet.iterate should sleep this much between each pool,
    # to not bombard the database with queries.
    subpolling_interval = 0.5

    task_cls = Task
    taskset_cls = TaskSet

    def __init__(self, dburi=None, engine_options=None, url=None, **kwargs):
        # The `url` argument was added later and is used by
        # the app to set backend by url (celery.app.backends.by_url)
        super().__init__(expires_type=maybe_timedelta,
                         url=url, **kwargs)
        conf = self.app.conf

        # Override retry defaults to preserve backward compatibility.
        # Previously, DatabaseBackend used a custom @retry decorator that always
        # retried with max_retries=3. We maintain this behavior by default.
        self.always_retry = conf.get('result_backend_always_retry', True)
        self.max_retries = conf.get('result_backend_max_retries', 3)

        if self.extended_result:
            self.task_cls = TaskExtended

        self.url = url or dburi or conf.database_url

        # Merge engine options: defaults from config <- constructor overrides
        # The defaults (pool_pre_ping=True, pool_recycle=3600) are defined in
        # celery/app/defaults.py under database_engine_options
        self.engine_options = dict(
            conf.database_engine_options or {},
            **(engine_options or {})
        )
        self.short_lived_sessions = kwargs.get(
            'short_lived_sessions',
            conf.database_short_lived_sessions)

        schemas = conf.database_table_schemas or {}
        tablenames = conf.database_table_names or {}
        self.task_cls.configure(
            schema=schemas.get('task'),
            name=tablenames.get('task'))
        self.taskset_cls.configure(
            schema=schemas.get('group'),
            name=tablenames.get('group'))

        if not self.url:
            raise ImproperlyConfigured(
                'Missing connection string! Do you have the'
                ' database_url setting set to a real value?')

        self.session_manager = SessionManager()

        create_tables_at_setup = conf.database_create_tables_at_setup
        if create_tables_at_setup is True:
            self._create_tables()

    @property
    def extended_result(self):
        return self.app.conf.find_value_for_key('extended', 'result')

    def exception_safe_to_retry(self, exc):
        return isinstance(exc, RETRYABLE_DB_ERRORS)

    def on_backend_retryable_error(self, exc):
        self.session_manager.invalidate(self.url)

    def _create_tables(self):
        """Create the task and taskset tables."""
        self.ResultSession()

    def ResultSession(self, session_manager=None):
        if session_manager is None:
            session_manager = self.session_manager
        return session_manager.session_factory(
            dburi=self.url,
            short_lived_sessions=self.short_lived_sessions,
            **self.engine_options)

    def _store_result(self, task_id, result, state, traceback=None,
                      request=None, **kwargs):
        """Store return value and state of an executed task."""
        session = self.ResultSession()
        with session_cleanup(session):
            task = list(session.query(self.task_cls).filter(self.task_cls.task_id == task_id))
            task = task and task[0]
            if not task:
                task = self.task_cls(task_id)
                task.task_id = task_id
                session.add(task)
                session.flush()

            self._update_result(task, result, state, traceback=traceback, request=request)
            session.commit()

    def _update_result(self, task, result, state, traceback=None,
                       request=None):

        meta = self._get_result_meta(result=result, state=state,
                                     traceback=traceback, request=request,
                                     format_date=False, encode=True)

        # Exclude the primary key id and task_id columns
        # as we should not set it None
        columns = [column.name for column in self.task_cls.__table__.columns
                   if column.name not in {'id', 'task_id'}]

        # Iterate through the columns name of the table
        # to set the value from meta.
        # If the value is not present in meta, set None
        for column in columns:
            value = meta.get(column)
            setattr(task, column, value)

    def _get_task_meta_for(self, task_id):
        """Get task meta-data for a task by id."""
        session = self.ResultSession()
        with session_cleanup(session):
            task = list(session.query(self.task_cls).filter(self.task_cls.task_id == task_id))
            task = task and task[0]
            if not task:
                task = self.task_cls(task_id)
                task.status = states.PENDING
                task.result = None
            data = task.to_dict()
            if data.get('args', None) is not None:
                data['args'] = self.decode(data['args'])
            if data.get('kwargs', None) is not None:
                data['kwargs'] = self.decode(data['kwargs'])
            return self.meta_from_decoded(data)

    def _save_group(self, group_id, result):
        """Store the result of an executed group."""
        session = self.ResultSession()
        with session_cleanup(session):
            group = self.taskset_cls(group_id, result)
            session.add(group)
            session.flush()
            session.commit()
            return result

    def _restore_group(self, group_id):
        """Get meta-data for group by id."""
        session = self.ResultSession()
        with session_cleanup(session):
            group = session.query(self.taskset_cls).filter(
                self.taskset_cls.taskset_id == group_id).first()
            if group:
                return group.to_dict()

    def _delete_group(self, group_id):
        """Delete meta-data for group by id."""
        session = self.ResultSession()
        with session_cleanup(session):
            session.query(self.taskset_cls).filter(
                self.taskset_cls.taskset_id == group_id).delete()
            session.flush()
            session.commit()

    def _forget(self, task_id):
        """Forget about result."""
        session = self.ResultSession()
        with session_cleanup(session):
            session.query(self.task_cls).filter(self.task_cls.task_id == task_id).delete()
            session.commit()

    def cleanup(self):
        """Delete expired meta-data."""
        session = self.ResultSession()
        expires = self.expires
        now = self.app.now()
        with session_cleanup(session):
            session.query(self.task_cls).filter(
                self.task_cls.date_done < (now - expires)).delete()
            session.query(self.taskset_cls).filter(
                self.taskset_cls.date_done < (now - expires)).delete()
            session.commit()

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        kwargs.update(
            {'dburi': self.url,
             'expires': self.expires,
             'engine_options': self.engine_options})
        return super().__reduce__(args, kwargs)
