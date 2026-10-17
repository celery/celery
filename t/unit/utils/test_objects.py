import gc
import weakref
from contextlib import contextmanager
from unittest.mock import MagicMock, Mock

import pytest

from celery.utils.objects import Bunch, FallbackContext


class test_Bunch:

    def test(self):
        x = Bunch(foo='foo', bar=2)
        assert x.foo == 'foo'
        assert x.bar == 2


class test_FallbackContext:

    def test_exit_uses_manager_not_returned_resource(self):
        manager = MagicMock()
        resource = MagicMock()
        manager.__enter__.return_value = resource

        with FallbackContext(None, lambda: manager) as value:
            assert value is resource

        manager.__exit__.assert_called_once_with(None, None, None)
        resource.__exit__.assert_not_called()

    def test_failed_enter_does_not_retain_manager(self):
        references = []

        class Manager:
            def __enter__(self):
                raise ValueError('enter failed')

            def __exit__(self, *exc_info):
                pytest.fail('failed entry must not call exit')

        def fallback():
            manager = Manager()
            references.append(weakref.ref(manager))
            return manager

        context = FallbackContext(None, fallback)
        with pytest.raises(ValueError, match='enter failed'):
            with context:
                pytest.fail('failed entry must not enter body')

        gc.collect()
        assert references[0]() is None

    @pytest.mark.parametrize('resource', [None, 'resource'])
    def test_fallback_lifetime(self, resource):
        events = []

        @contextmanager
        def fallback(value, *, name):
            events.append(('enter', name))
            try:
                yield value
            finally:
                events.append(('exit', name))

        with FallbackContext(None, fallback, resource, name='fallback') as value:
            assert value is resource
            assert events == [('enter', 'fallback')]

        assert events == [('enter', 'fallback'), ('exit', 'fallback')]

    @pytest.mark.parametrize('suppress', [False, True])
    def test_fallback_receives_exception(self, suppress):
        error = ValueError('body failed')
        received = []

        @contextmanager
        def fallback():
            try:
                yield 'resource'
            except ValueError as exc:
                received.append(exc)
                if not suppress:
                    raise

        def run():
            with FallbackContext(None, fallback):
                raise error

        if suppress:
            run()
        else:
            with pytest.raises(ValueError) as exc_info:
                run()
            assert exc_info.value is error

        assert received == [error]

    def test_provided_resource_is_not_managed(self):
        resource = Mock()
        fallback = Mock()

        with FallbackContext(resource, fallback) as value:
            assert value is resource

        fallback.assert_not_called()
        assert not resource.mock_calls
