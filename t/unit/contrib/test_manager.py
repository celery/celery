from io import StringIO
from unittest.mock import Mock

import pytest

from celery.contrib.testing.manager import Manager


class test_Manager:
    @pytest.mark.parametrize('emit_warning', [False, True])
    def test_wait_for_retries_with_optional_warning(self, emit_warning):
        stdout, stderr = StringIO(), StringIO()
        manager = Manager(self.app, stdout=stdout, stderr=stderr)
        error = ValueError('not ready')
        callback = Mock(side_effect=[error, 'ready'])
        errback = Mock()

        result = manager.wait_for(
            callback, ValueError, desc='result', emit_warning=emit_warning,
            errback=errback, max_retries=1, interval_start=0,
            interval_step=0, interval_max=0,
        )

        assert result == 'ready'
        assert callback.call_count == 2
        errback.assert_called_once_with(error, 0, 0)
        assert stdout.getvalue() == ''
        expected = "Still waiting for result.  Trying again now: ValueError('not ready')\n"
        assert stderr.getvalue() == (expected if emit_warning else '')

    def test_wait_for_preserves_exception_after_retry_limit(self):
        stderr = StringIO()
        manager = Manager(self.app, stderr=stderr)
        error = ValueError('not ready')
        callback = Mock(side_effect=error)

        with pytest.raises(ValueError) as exc_info:
            manager.wait_for(
                callback, ValueError, emit_warning=True, max_retries=1,
                interval_start=0, interval_step=0, interval_max=0,
            )

        assert exc_info.value is error
        assert callback.call_count == 2
        assert stderr.getvalue() == (
            "Still waiting for thing.  Trying again now: ValueError('not ready')\n"
        )

    def test_ensure_not_for_a_while_emits_warning(self):
        stderr = StringIO()
        manager = Manager(self.app, stderr=stderr)
        callback = Mock(side_effect=ValueError('not ready'))

        result = manager.ensure_not_for_a_while(
            callback, ValueError, emit_warning=True, max_retries=1,
            interval_start=0, interval_step=0,
        )

        assert result is None
        assert callback.call_count == 2
        assert stderr.getvalue() == (
            "Still waiting for thing.  Trying again now: ValueError('not ready')\n"
        )
