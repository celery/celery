from datetime import datetime
from pickle import dumps, loads
from unittest.mock import Mock, patch

import pytest

from celery import states, uuid
from celery.app.task import Context
from celery.exceptions import ImproperlyConfigured

pytest.importorskip('sqlalchemy')

from celery.backends.database import (DatabaseBackend, retry, session,  # noqa
                                      session_cleanup)
from celery.backends.database.models import Task, TaskSet  # noqa
from celery.backends.database.session import (  # noqa
    PREPARE_MODELS_MAX_RETRIES, ResultModelBase, SessionManager)
from t import skip  # noqa


class SomeClass:

    def __init__(self, data):
        self.data = data

    def __eq__(self, cmp):
        return self.data == cmp.data


class test_session_cleanup:

    def test_context(self):
        session = Mock(name='session')
        with session_cleanup(session):
            pass
        session.close.assert_called_with()

    def test_context_raises(self):
        session = Mock(name='session')
        with pytest.raises(KeyError):
            with session_cleanup(session):
                raise KeyError()
        session.rollback.assert_called_with()
        session.close.assert_called_with()


@skip.if_pypy
class test_DatabaseBackend:

    def setup(self):
        self.uri = 'sqlite:///test.db'
        self.app.conf.result_serializer = 'pickle'

    def test_retry_helper(self):
        from celery.backends.database import DatabaseError

        calls = [0]

        @retry
        def raises():
            calls[0] += 1
            raise DatabaseError(1, 2, 3)

        with pytest.raises(DatabaseError):
            raises(max_retries=5)
        assert calls[0] == 5

    def test_missing_dburi_raises_ImproperlyConfigured(self):
        self.app.conf.database_url = None
        with pytest.raises(ImproperlyConfigured):
            DatabaseBackend(app=self.app)

    def test_table_schema_config(self):
        self.app.conf.database_table_schemas = {
            'task': 'foo',
            'group': 'bar',
        }
        tb = DatabaseBackend(self.uri, app=self.app)
        assert tb.task_cls.__table__.schema == 'foo'
        assert tb.task_cls.__table__.c.id.default.schema == 'foo'
        assert tb.taskset_cls.__table__.schema == 'bar'
        assert tb.taskset_cls.__table__.c.id.default.schema == 'bar'

    def test_table_name_config(self):
        self.app.conf.database_table_names = {
            'task': 'foo',
            'group': 'bar',
        }
        tb = DatabaseBackend(self.uri, app=self.app)
        assert tb.task_cls.__table__.name == 'foo'
        assert tb.taskset_cls.__table__.name == 'bar'

    def test_missing_task_id_is_PENDING(self):
        tb = DatabaseBackend(self.uri, app=self.app)
        assert tb.get_state('xxx-does-not-exist') == states.PENDING

    def test_missing_task_meta_is_dict_with_pending(self):
        tb = DatabaseBackend(self.uri, app=self.app)
        meta = tb.get_task_meta('xxx-does-not-exist-at-all')
        assert meta['status'] == states.PENDING
        assert meta['task_id'] == 'xxx-does-not-exist-at-all'
        assert meta['result'] is None
        assert meta['traceback'] is None

    def test_mark_as_done(self):
        tb = DatabaseBackend(self.uri, app=self.app)

        tid = uuid()

        assert tb.get_state(tid) == states.PENDING
        assert tb.get_result(tid) is None

        tb.mark_as_done(tid, 42)
        assert tb.get_state(tid) == states.SUCCESS
        assert tb.get_result(tid) == 42

    def test_is_pickled(self):
        tb = DatabaseBackend(self.uri, app=self.app)

        tid2 = uuid()
        result = {'foo': 'baz', 'bar': SomeClass(12345)}
        tb.mark_as_done(tid2, result)
        # is serialized properly.
        rindb = tb.get_result(tid2)
        assert rindb.get('foo') == 'baz'
        assert rindb.get('bar').data == 12345

    def test_mark_as_started(self):
        tb = DatabaseBackend(self.uri, app=self.app)
        tid = uuid()
        tb.mark_as_started(tid)
        assert tb.get_state(tid) == states.STARTED

    def test_mark_as_revoked(self):
        tb = DatabaseBackend(self.uri, app=self.app)
        tid = uuid()
        tb.mark_as_revoked(tid)
        assert tb.get_state(tid) == states.REVOKED

    def test_mark_as_retry(self):
        tb = DatabaseBackend(self.uri, app=self.app)
        tid = uuid()
        try:
            raise KeyError('foo')
        except KeyError as exception:
            import traceback
            trace = '\n'.join(traceback.format_stack())
            tb.mark_as_retry(tid, exception, traceback=trace)
            assert tb.get_state(tid) == states.RETRY
            assert isinstance(tb.get_result(tid), KeyError)
            assert tb.get_traceback(tid) == trace

    def test_mark_as_failure(self):
        tb = DatabaseBackend(self.uri, app=self.app)

        tid3 = uuid()
        try:
            raise KeyError('foo')
        except KeyError as exception:
            import traceback
            trace = '\n'.join(traceback.format_stack())
            tb.mark_as_failure(tid3, exception, traceback=trace)
            assert tb.get_state(tid3) == states.FAILURE
            assert isinstance(tb.get_result(tid3), KeyError)
            assert tb.get_traceback(tid3) == trace

    def test_forget(self):
        tb = DatabaseBackend(self.uri, backend='memory://', app=self.app)
        tid = uuid()
        tb.mark_as_done(tid, {'foo': 'bar'})
        tb.mark_as_done(tid, {'foo': 'bar'})
        x = self.app.AsyncResult(tid, backend=tb)
        x.forget()
        assert x.result is None

    def test_process_cleanup(self):
        tb = DatabaseBackend(self.uri, app=self.app)
        tb.process_cleanup()

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce(self):
        tb = DatabaseBackend(self.uri, app=self.app)
        assert loads(dumps(tb))

    def test_save__restore__delete_group(self):
        tb = DatabaseBackend(self.uri, app=self.app)

        tid = uuid()
        res = {'something': 'special'}
        assert tb.save_group(tid, res) == res

        res2 = tb.restore_group(tid)
        assert res2 == res

        tb.delete_group(tid)
        assert tb.restore_group(tid) is None

        assert tb.restore_group('xxx-nonexisting-id') is None

    def test_cleanup(self):
        tb = DatabaseBackend(self.uri, app=self.app)
        for i in range(10):
            tb.mark_as_done(uuid(), 42)
            tb.save_group(uuid(), {'foo': 'bar'})
        s = tb.ResultSession()
        for t in s.query(Task).all():
            t.date_done = datetime.now() - tb.expires * 2
        for t in s.query(TaskSet).all():
            t.date_done = datetime.now() - tb.expires * 2
        s.commit()
        s.close()

        tb.cleanup()

    def test_Task__repr__(self):
        assert 'foo' in repr(Task('foo'))

    def test_TaskSet__repr__(self):
        assert 'foo', repr(TaskSet('foo' in None))


@skip.if_pypy
class test_DatabaseBackend_result_extended():
    def setup(self):
        self.uri = 'sqlite:///test.db'
        self.app.conf.result_serializer = 'pickle'
        self.app.conf.result_extended = True

    @pytest.mark.parametrize(
        'result_serializer, args, kwargs',
        [
            ('pickle', (SomeClass(1), SomeClass(2)), {'foo': SomeClass(123)}),
            ('json', ['a', 'b'], {'foo': 'bar'}),
        ],
        ids=['using pickle', 'using json']
    )
    def test_store_result(self, result_serializer, args, kwargs):
        self.app.conf.result_serializer = result_serializer
        tb = DatabaseBackend(self.uri, app=self.app)
        tid = uuid()

        request = Context(args=args, kwargs=kwargs,
                          task='mytask', retries=2,
                          hostname='celery@worker_1',
                          delivery_info={'routing_key': 'celery'})

        tb.store_result(tid, {'fizz': 'buzz'}, states.SUCCESS, request=request)
        meta = tb.get_task_meta(tid)

        assert meta['result'] == {'fizz': 'buzz'}
        assert meta['args'] == args
        assert meta['kwargs'] == kwargs
        assert meta['queue'] == 'celery'
        assert meta['name'] == 'mytask'
        assert meta['retries'] == 2
        assert meta['worker'] == "celery@worker_1"

    @pytest.mark.parametrize(
        'result_serializer, args, kwargs',
        [
            ('pickle', (SomeClass(1), SomeClass(2)), {'foo': SomeClass(123)}),
            ('json', ['a', 'b'], {'foo': 'bar'}),
        ],
        ids=['using pickle', 'using json']
    )
    def test_store_none_result(self, result_serializer, args, kwargs):
        self.app.conf.result_serializer = result_serializer
        tb = DatabaseBackend(self.uri, app=self.app)
        tid = uuid()

        request = Context(args=args, kwargs=kwargs,
                          task='mytask', retries=2,
                          hostname='celery@worker_1',
                          delivery_info={'routing_key': 'celery'})

        tb.store_result(tid, None, states.SUCCESS, request=request)
        meta = tb.get_task_meta(tid)

        assert meta['result'] is None
        assert meta['args'] == args
        assert meta['kwargs'] == kwargs
        assert meta['queue'] == 'celery'
        assert meta['name'] == 'mytask'
        assert meta['retries'] == 2
        assert meta['worker'] == "celery@worker_1"

    @pytest.mark.parametrize(
        'result_serializer, args, kwargs',
        [
            ('pickle', (SomeClass(1), SomeClass(2)),
             {'foo': SomeClass(123)}),
            ('json', ['a', 'b'], {'foo': 'bar'}),
        ],
        ids=['using pickle', 'using json']
    )
    def test_get_result_meta(self, result_serializer, args, kwargs):
        self.app.conf.result_serializer = result_serializer
        tb = DatabaseBackend(self.uri, app=self.app)

        request = Context(args=args, kwargs=kwargs,
                          task='mytask', retries=2,
                          hostname='celery@worker_1',
                          delivery_info={'routing_key': 'celery'})

        meta = tb._get_result_meta(result={'fizz': 'buzz'},
                                   state=states.SUCCESS, traceback=None,
                                   request=request, format_date=False,
                                   encode=True)

        assert meta['result'] == {'fizz': 'buzz'}
        assert tb.decode(meta['args']) == args
        assert tb.decode(meta['kwargs']) == kwargs
        assert meta['queue'] == 'celery'
        assert meta['name'] == 'mytask'
        assert meta['retries'] == 2
        assert meta['worker'] == "celery@worker_1"

    @pytest.mark.parametrize(
        'result_serializer, args, kwargs',
        [
            ('pickle', (SomeClass(1), SomeClass(2)),
             {'foo': SomeClass(123)}),
            ('json', ['a', 'b'], {'foo': 'bar'}),
        ],
        ids=['using pickle', 'using json']
    )
    def test_get_result_meta_with_none(self, result_serializer, args, kwargs):
        self.app.conf.result_serializer = result_serializer
        tb = DatabaseBackend(self.uri, app=self.app)

        request = Context(args=args, kwargs=kwargs,
                          task='mytask', retries=2,
                          hostname='celery@worker_1',
                          delivery_info={'routing_key': 'celery'})

        meta = tb._get_result_meta(result=None,
                                   state=states.SUCCESS, traceback=None,
                                   request=request, format_date=False,
                                   encode=True)

        assert meta['result'] is None
        assert tb.decode(meta['args']) == args
        assert tb.decode(meta['kwargs']) == kwargs
        assert meta['queue'] == 'celery'
        assert meta['name'] == 'mytask'
        assert meta['retries'] == 2
        assert meta['worker'] == "celery@worker_1"


class test_SessionManager:

    def test_after_fork(self):
        s = SessionManager()
        assert not s.forked
        s._after_fork()
        assert s.forked

    @patch('celery.backends.database.session.create_engine')
    def test_get_engine_forked(self, create_engine):
        s = SessionManager()
        s._after_fork()
        engine = s.get_engine('dburi', foo=1)
        create_engine.assert_called_with('dburi', foo=1)
        assert engine is create_engine()
        engine2 = s.get_engine('dburi', foo=1)
        assert engine2 is engine

    @patch('celery.backends.database.session.create_engine')
    def test_get_engine_kwargs(self, create_engine):
        s = SessionManager()
        engine = s.get_engine('dbur', foo=1, pool_size=5)
        assert engine is create_engine()
        engine2 = s.get_engine('dburi', foo=1)
        assert engine2 is engine

    @patch('celery.backends.database.session.sessionmaker')
    def test_create_session_forked(self, sessionmaker):
        s = SessionManager()
        s.get_engine = Mock(name='get_engine')
        s._after_fork()
        engine, session = s.create_session('dburi', short_lived_sessions=True)
        sessionmaker.assert_called_with(bind=s.get_engine())
        assert session is sessionmaker()
        sessionmaker.return_value = Mock(name='new')
        engine, session2 = s.create_session('dburi', short_lived_sessions=True)
        sessionmaker.assert_called_with(bind=s.get_engine())
        assert session2 is not session
        sessionmaker.return_value = Mock(name='new2')
        engine, session3 = s.create_session(
            'dburi', short_lived_sessions=False)
        sessionmaker.assert_called_with(bind=s.get_engine())
        assert session3 is session2

    def test_coverage_madness(self):
        prev, session.register_after_fork = (
            session.register_after_fork, None,
        )
        try:
            SessionManager()
        finally:
            session.register_after_fork = prev

    @patch('celery.backends.database.session.create_engine')
    def test_prepare_models_terminates(self, create_engine):
        """SessionManager.prepare_models has retry logic because the creation
        of database tables by multiple workers is racy. This test patches
        the used method to always raise, so we can verify that it does
        eventually terminate.
        """
        from sqlalchemy.dialects.sqlite import dialect
        from sqlalchemy.exc import DatabaseError

        sqlite = dialect.dbapi()
        manager = SessionManager()
        engine = manager.get_engine('dburi')

        def raise_err(bind):
            raise DatabaseError("", "", [], sqlite.DatabaseError)

        patch_create_all = patch.object(
            ResultModelBase.metadata, 'create_all', side_effect=raise_err)

        with pytest.raises(DatabaseError), patch_create_all as mock_create_all:
            manager.prepare_models(engine)

        assert mock_create_all.call_count == PREPARE_MODELS_MAX_RETRIES + 1
