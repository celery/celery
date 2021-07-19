import threading
from contextlib import contextmanager
from unittest.mock import patch

import pytest

import celery.contrib.testing.worker as contrib_embed_worker
from celery.app import backends
from celery.backends.cache import CacheBackend
from celery.exceptions import ImproperlyConfigured
from celery.utils.nodenames import anon_nodename


class CachedBackendWithTreadTrucking(CacheBackend):
    test_instance_count = 0
    test_call_stats = {}

    def _track_attribute_access(self, method_name):
        cls = type(self)

        instance_no = getattr(self, '_instance_no', None)
        if instance_no is None:
            instance_no = self._instance_no = cls.test_instance_count
            cls.test_instance_count += 1
            cls.test_call_stats[instance_no] = []

        cls.test_call_stats[instance_no].append({
            'thread_id': threading.get_ident(),
            'method_name': method_name
        })

    def __getattribute__(self, name):
        if name == '_instance_no' or name == '_track_attribute_access':
            return super().__getattribute__(name)

        if name.startswith('__') and name != '__init__':
            return super().__getattribute__(name)

        self._track_attribute_access(name)
        return super().__getattribute__(name)


@contextmanager
def embed_worker(app,
                 concurrency=1,
                 pool='threading', **kwargs):
    """
    Helper embedded worker for testing.

    It's based on a :func:`celery.contrib.testing.worker.start_worker`,
    but doesn't modifies logging settings and additionally shutdown
    worker pool.
    """
    # prepare application for worker
    app.finalize()
    app.set_current()

    worker = contrib_embed_worker.TestWorkController(
        app=app,
        concurrency=concurrency,
        hostname=anon_nodename(),
        pool=pool,
        # not allowed to override TestWorkController.on_consumer_ready
        ready_callback=None,
        without_heartbeat=kwargs.pop("without_heartbeat", True),
        without_mingle=True,
        without_gossip=True,
        **kwargs
    )

    t = threading.Thread(target=worker.start, daemon=True)
    t.start()
    worker.ensure_started()

    yield worker

    worker.stop()
    t.join(10.0)
    if t.is_alive():
        raise RuntimeError(
            "Worker thread failed to exit within the allocated timeout. "
            "Consider raising `shutdown_timeout` if your tasks take longer "
            "to execute."
        )


class test_backends:

    @pytest.mark.parametrize('url,expect_cls', [
        ('cache+memory://', CacheBackend),
    ])
    def test_get_backend_aliases(self, url, expect_cls, app):
        backend, url = backends.by_url(url, app.loader)
        assert isinstance(backend(app=app, url=url), expect_cls)

    def test_unknown_backend(self, app):
        with pytest.raises(ImportError):
            backends.by_name('fasodaopjeqijwqe', app.loader)

    def test_backend_by_url(self, app, url='redis://localhost/1'):
        from celery.backends.redis import RedisBackend
        backend, url_ = backends.by_url(url, app.loader)
        assert backend is RedisBackend
        assert url_ == url

    def test_sym_raises_ValuError(self, app):
        with patch('celery.app.backends.symbol_by_name') as sbn:
            sbn.side_effect = ValueError()
            with pytest.raises(ImproperlyConfigured):
                backends.by_name('xxx.xxx:foo', app.loader)

    def test_backend_can_not_be_module(self, app):
        with pytest.raises(ImproperlyConfigured):
            backends.by_name(pytest, app.loader)

    @pytest.mark.celery(
        result_backend=f'{CachedBackendWithTreadTrucking.__module__}.'
                       f'{CachedBackendWithTreadTrucking.__qualname__}'
                       f'+memory://')
    def test_backend_thread_safety(self):
        @self.app.task
        def dummy_add_task(x, y):
            return x + y

        with embed_worker(app=self.app, pool='threads'):
            result = dummy_add_task.delay(6, 9)
            assert result.get(timeout=10) == 15

        call_stats = CachedBackendWithTreadTrucking.test_call_stats
        # check that backend instance is used without same thread
        for backend_call_stats in call_stats.values():
            thread_ids = set()
            for call_stat in backend_call_stats:
                thread_ids.add(call_stat['thread_id'])
            assert len(thread_ids) <= 1, \
                "The same celery backend instance is used by multiple threads"
