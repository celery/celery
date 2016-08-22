from __future__ import absolute_import, unicode_literals

from time import time

from case import Mock, patch

from celery.events.dumper import (
    humanize_type,
    Dumper,
    evdump,
)
from celery.five import WhateverIO


class test_Dumper:

    def setup(self):
        self.out = WhateverIO()
        self.dumper = Dumper(out=self.out)

    def test_humanize_type(self):
        assert humanize_type('worker-offline') == 'shutdown'
        assert humanize_type('task-started') == 'task started'

    def test_format_task_event(self):
        self.dumper.format_task_event(
            'worker@example.com', time(), 'task-started', 'tasks.add', {})
        assert self.out.getvalue()

    def test_on_event(self):
        event = {
            'hostname': 'worker@example.com',
            'timestamp': time(),
            'uuid': '1ef',
            'name': 'tasks.add',
            'args': '(2, 2)',
            'kwargs': '{}',
        }
        self.dumper.on_event(dict(event, type='task-received'))
        assert self.out.getvalue()
        self.dumper.on_event(dict(event, type='task-revoked'))
        self.dumper.on_event(dict(event, type='worker-online'))

    @patch('celery.events.EventReceiver.capture')
    def test_evdump(self, capture):
        capture.side_effect = KeyboardInterrupt()
        evdump(app=self.app)

    def test_evdump_error_handler(self):
        app = Mock(name='app')
        with patch('celery.events.dumper.Dumper') as Dumper:
            Dumper.return_value = Mock(name='dumper')
            recv = app.events.Receiver.return_value = Mock()

            def se(*_a, **_k):
                recv.capture.side_effect = SystemExit()
                raise KeyError()
            recv.capture.side_effect = se

            Conn = app.connection_for_read.return_value = Mock(name='conn')
            conn = Conn.clone.return_value = Mock(name='cloned_conn')
            conn.connection_errors = (KeyError,)
            conn.channel_errors = ()

            evdump(app)
            conn.ensure_connection.assert_called()
            errback = conn.ensure_connection.call_args[0][0]
            errback(KeyError(), 1)
            conn.as_uri.assert_called()
