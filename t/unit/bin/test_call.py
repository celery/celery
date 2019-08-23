from __future__ import absolute_import, unicode_literals

from datetime import datetime

import pytest
from kombu.utils.json import dumps

from case import patch
from celery.bin.call import call
from celery.five import WhateverIO


class test_call:

    def setup(self):

        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        self.add = add

    @patch('celery.app.base.Celery.send_task')
    def test_run(self, send_task):
        a = call(app=self.app, stderr=WhateverIO(), stdout=WhateverIO())
        a.run(self.add.name)
        send_task.assert_called()

        a.run(self.add.name,
              args=dumps([4, 4]),
              kwargs=dumps({'x': 2, 'y': 2}))
        assert send_task.call_args[1]['args'], [4 == 4]
        assert send_task.call_args[1]['kwargs'] == {'x': 2, 'y': 2}

        a.run(self.add.name, expires=10, countdown=10)
        assert send_task.call_args[1]['expires'] == 10
        assert send_task.call_args[1]['countdown'] == 10

        now = datetime.now()
        iso = now.isoformat()
        a.run(self.add.name, expires=iso)
        assert send_task.call_args[1]['expires'] == now
        with pytest.raises(ValueError):
            a.run(self.add.name, expires='foobaribazibar')
