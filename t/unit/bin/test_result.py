from __future__ import absolute_import, unicode_literals

from case import patch
from celery.bin.result import result
from celery.five import WhateverIO


class test_result:

    def setup(self):

        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        self.add = add

    def test_run(self):
        with patch('celery.result.AsyncResult.get') as get:
            out = WhateverIO()
            r = result(app=self.app, stdout=out)
            get.return_value = 'Jerry'
            r.run('id')
            assert 'Jerry' in out.getvalue()

            get.return_value = 'Elaine'
            r.run('id', task=self.add.name)
            assert 'Elaine' in out.getvalue()

            with patch('celery.result.AsyncResult.traceback') as tb:
                r.run('id', task=self.add.name, traceback=True)
                assert str(tb) in out.getvalue()
