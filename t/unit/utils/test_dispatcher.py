from __future__ import absolute_import, unicode_literals


import gc
import sys
import time

from celery.utils.dispatch import Signal


if sys.platform.startswith('java'):

    def garbage_collect():
        # Some JVM GCs will execute finalizers in a different thread, meaning
        # we need to wait for that to complete before we go on looking for the
        # effects of that.
        gc.collect()
        time.sleep(0.1)

elif hasattr(sys, 'pypy_version_info'):

    def garbage_collect():  # noqa
        # Collecting weakreferences can take two collections on PyPy.
        gc.collect()
        gc.collect()
else:

    def garbage_collect():  # noqa
        gc.collect()


def receiver_1_arg(val, **kwargs):
    return val


class Callable(object):

    def __call__(self, val, **kwargs):
        return val

    def a(self, val, **kwargs):
        return val

a_signal = Signal(providing_args=['val'])


class test_Signal:
    """Test suite for dispatcher (barely started)"""

    def _testIsClean(self, signal):
        """Assert that everything has been cleaned up automatically"""
        assert signal.receivers == []

        # force cleanup just in case
        signal.receivers = []

    def test_exact(self):
        a_signal.connect(receiver_1_arg, sender=self)
        try:
            expected = [(receiver_1_arg, 'test')]
            result = a_signal.send(sender=self, val='test')
            assert result == expected
        finally:
            a_signal.disconnect(receiver_1_arg, sender=self)
        self._testIsClean(a_signal)

    def test_ignored_sender(self):
        a_signal.connect(receiver_1_arg)
        try:
            expected = [(receiver_1_arg, 'test')]
            result = a_signal.send(sender=self, val='test')
            assert result == expected
        finally:
            a_signal.disconnect(receiver_1_arg)
        self._testIsClean(a_signal)

    def test_garbage_collected(self):
        a = Callable()
        a_signal.connect(a.a, sender=self)
        expected = []
        del a
        garbage_collect()
        result = a_signal.send(sender=self, val='test')
        assert result == expected
        self._testIsClean(a_signal)

    def test_multiple_registration(self):
        a = Callable()
        result = None
        try:
            a_signal.connect(a)
            a_signal.connect(a)
            a_signal.connect(a)
            a_signal.connect(a)
            a_signal.connect(a)
            a_signal.connect(a)
            result = a_signal.send(sender=self, val='test')
            assert len(result) == 1
            assert len(a_signal.receivers) == 1
        finally:
            del a
            del result
            garbage_collect()
            self._testIsClean(a_signal)

    def test_uid_registration(self):

        def uid_based_receiver_1(**kwargs):
            pass

        def uid_based_receiver_2(**kwargs):
            pass

        a_signal.connect(uid_based_receiver_1, dispatch_uid='uid')
        try:
            a_signal.connect(uid_based_receiver_2, dispatch_uid='uid')
            assert len(a_signal.receivers) == 1
        finally:
            a_signal.disconnect(dispatch_uid='uid')
        self._testIsClean(a_signal)

    def test_robust(self):

        def fails(val, **kwargs):
            raise ValueError('this')

        a_signal.connect(fails)
        try:
            a_signal.send(sender=self, val='test')
        finally:
            a_signal.disconnect(fails)
        self._testIsClean(a_signal)

    def test_disconnection(self):
        receiver_1 = Callable()
        receiver_2 = Callable()
        receiver_3 = Callable()
        try:
            try:
                a_signal.connect(receiver_1)
                a_signal.connect(receiver_2)
                a_signal.connect(receiver_3)
            finally:
                a_signal.disconnect(receiver_1)
            del receiver_2
            garbage_collect()
        finally:
            a_signal.disconnect(receiver_3)
        self._testIsClean(a_signal)
