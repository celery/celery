from __future__ import absolute_import

from mock import patch
from time import time

from celery.events.dumper import (
    humanize_type,
    Dumper,
    evdump,
)

from celery.tests.utils import Case, WhateverIO


class test_Dumper(Case):

    def setUp(self):
        self.out = WhateverIO()
        self.dumper = Dumper(out=self.out)

    def test_humanize_type(self):
        self.assertEqual(humanize_type('worker-offline'), 'shutdown')
        self.assertEqual(humanize_type('task-started'), 'task started')

    def test_format_task_event(self):
        self.dumper.format_task_event(
            'worker.example.com', time(), 'task-started', 'tasks.add', {})
        self.assertTrue(self.out.getvalue())

    def test_on_event(self):
        event = {
            'hostname': 'worker.example.com',
            'timestamp': time(),
            'uuid': '1ef',
            'name': 'tasks.add',
            'args': '(2, 2)',
            'kwargs': '{}',
        }
        self.dumper.on_event(dict(event, type='task-received'))
        self.assertTrue(self.out.getvalue())
        self.dumper.on_event(dict(event, type='task-revoked'))
        self.dumper.on_event(dict(event, type='worker-online'))

    @patch('celery.events.EventReceiver.capture')
    def test_evdump(self, capture):
        capture.side_effect = KeyboardInterrupt()
        evdump()
