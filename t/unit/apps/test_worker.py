from unittest.mock import Mock, patch

import pytest

from celery.apps.worker import Worker


class test_Worker_purge:
    """Purging at startup must honour the broker connection retry settings.

    ``purge_messages`` runs from ``on_start``, before the consumer blueprint
    exists, so the retry handling in :mod:`celery.worker.consumer` never sees
    it.  See https://github.com/celery/celery/issues/10102.
    """

    @pytest.fixture(autouse=True)
    def _setup_app(self, app):
        self.app = app

    def _worker(self, **conf):
        self.app.conf.update(conf)
        worker = Worker(app=self.app, hostname='test@example.com')
        return worker

    def test_purge_retries_connection_on_startup(self):
        worker = self._worker(
            broker_connection_retry_on_startup=True,
            broker_connection_max_retries=7,
        )
        connection = Mock(name='connection')

        with patch.object(self.app, 'connection_for_write') as conn_for_write:
            conn_for_write.return_value.__enter__ = Mock(return_value=connection)
            conn_for_write.return_value.__exit__ = Mock(return_value=None)
            with patch.object(self.app.control, 'purge', return_value=0):
                worker.purge_messages()

        connection.ensure_connection.assert_called_once()
        assert connection.ensure_connection.call_args[0][1] == 7
        connection.connect.assert_not_called()

    def test_purge_does_not_retry_when_disabled(self):
        worker = self._worker(
            broker_connection_retry_on_startup=False,
        )
        connection = Mock(name='connection')

        with patch.object(self.app, 'connection_for_write') as conn_for_write:
            conn_for_write.return_value.__enter__ = Mock(return_value=connection)
            conn_for_write.return_value.__exit__ = Mock(return_value=None)
            with patch.object(self.app.control, 'purge', return_value=0):
                worker.purge_messages()

        connection.connect.assert_called_once_with()
        connection.ensure_connection.assert_not_called()

    def test_purge_falls_back_to_broker_connection_retry(self):
        # Apps that never set the newer setting keep the old one's behaviour.
        worker = self._worker(
            broker_connection_retry_on_startup=None,
            broker_connection_retry=True,
        )
        connection = Mock(name='connection')

        with patch.object(self.app, 'connection_for_write') as conn_for_write:
            conn_for_write.return_value.__enter__ = Mock(return_value=connection)
            conn_for_write.return_value.__exit__ = Mock(return_value=None)
            with patch.object(self.app.control, 'purge', return_value=0):
                worker.purge_messages()

        connection.ensure_connection.assert_called_once()
        connection.connect.assert_not_called()
