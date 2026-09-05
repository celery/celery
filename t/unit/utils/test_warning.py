"""Tests for celery.utils.warning module."""
from unittest.mock import patch

from celery.utils.warning import (is_eventlet_monkey_patched, is_eventlet_pool, is_gevent_monkey_patched,
                                  is_gevent_pool)


class test_monkey_patch_detection:

    def test_detect_gevent_monkey_patched_when_not_patched(self):
        """Test detect_gevent_monkey_patched returns False when gevent is not monkey patched."""
        from types import SimpleNamespace

        fake_monkey = SimpleNamespace()
        fake_monkey.is_module_patched = lambda *args, **kwargs: False
        fake_gevent = SimpleNamespace(monkey=fake_monkey)

        with patch.dict('sys.modules', {'gevent': fake_gevent, 'gevent.monkey': fake_monkey}):
            result = is_gevent_monkey_patched()

        assert result is False

    def test_detect_gevent_monkey_patched_when_patched(self):
        """Test is_gevent_monkey_patched returns True when gevent is monkey patched."""
        from types import SimpleNamespace

        fake_monkey = SimpleNamespace()
        fake_monkey.is_module_patched = lambda *args, **kwargs: True
        fake_gevent = SimpleNamespace(monkey=fake_monkey)

        with patch.dict('sys.modules', {'gevent': fake_gevent, 'gevent.monkey': fake_monkey}):
            result = is_gevent_monkey_patched()

        assert result is True

    def test_detect_gevent_monkey_patched_import_error(self):
        """Test detect_gevent_monkey_patched returns False when gevent import fails."""
        import builtins

        real_import = builtins.__import__

        def failing_import(name, *args, **kwargs):
            if name.startswith('gevent'):
                raise ImportError("Simulated gevent import failure")
            return real_import(name, *args, **kwargs)

        with patch.dict('sys.modules', {'gevent': None, 'gevent.monkey': None}):
            with patch('builtins.__import__', side_effect=failing_import):
                result = is_gevent_monkey_patched()

        assert result is False

    def test_detect_eventlet_monkey_patched_when_not_patched(self):
        """Test detect_eventlet_monkey_patched returns False when eventlet is not monkey patched."""
        from types import SimpleNamespace

        fake_patcher = SimpleNamespace()
        fake_patcher.is_monkey_patched = lambda *args, **kwargs: False
        fake_eventlet = SimpleNamespace(patcher=fake_patcher)

        with patch.dict('sys.modules', {'eventlet': fake_eventlet, 'eventlet.patcher': fake_patcher}):
            result = is_eventlet_monkey_patched()

        assert result is False

    def test_detect_eventlet_monkey_patched_when_patched(self):
        """Test is_eventlet_monkey_patched returns True when eventlet is monkey patched."""
        from types import SimpleNamespace

        fake_patcher = SimpleNamespace()
        fake_patcher.is_monkey_patched = lambda *args, **kwargs: True
        fake_eventlet = SimpleNamespace(patcher=fake_patcher)

        with patch.dict('sys.modules', {'eventlet': fake_eventlet, 'eventlet.patcher': fake_patcher}):
            result = is_eventlet_monkey_patched()

        assert result is True

    def test_detect_eventlet_monkey_patched_import_error(self):
        """Test detect_eventlet_monkey_patched returns False when eventlet import fails."""
        import builtins

        real_import = builtins.__import__

        def failing_import(name, *args, **kwargs):
            if name.startswith('eventlet'):
                raise ImportError("Simulated eventlet import failure")
            return real_import(name, *args, **kwargs)

        with patch.dict('sys.modules', {'eventlet': None, 'eventlet.patcher': None}):
            with patch('builtins.__import__', side_effect=failing_import):
                result = is_eventlet_monkey_patched()

        assert result is False


class test_pool_detection:

    def test_is_gevent_pool_returns_true_when_pool_matches(self):
        """Test is_gevent_pool returns True when pool_module matches gevent."""
        assert is_gevent_pool('celery.concurrency.gevent') is True

    def test_is_gevent_pool_returns_false_when_pool_differs(self):
        """Test is_gevent_pool returns False when pool_module doesn't match."""
        assert is_gevent_pool('celery.concurrency.prefork') is False
        assert is_gevent_pool('celery.concurrency.eventlet') is False
        assert is_gevent_pool('') is False

    def test_is_eventlet_pool_returns_true_when_pool_matches(self):
        """Test is_eventlet_pool returns True when pool_module matches eventlet."""
        assert is_eventlet_pool('celery.concurrency.eventlet') is True

    def test_is_eventlet_pool_returns_false_when_pool_differs(self):
        """Test is_eventlet_pool returns False when pool_module doesn't match."""
        assert is_eventlet_pool('celery.concurrency.prefork') is False
        assert is_eventlet_pool('celery.concurrency.gevent') is False
        assert is_eventlet_pool('') is False
