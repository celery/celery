from __future__ import absolute_import, unicode_literals

from case import Mock
from celery.bin.purge import purge
from celery.five import WhateverIO


class test_purge:

    def test_run(self):
        out = WhateverIO()
        a = purge(app=self.app, stdout=out)
        a._purge = Mock(name='_purge')
        a._purge.return_value = 0
        a.run(force=True)
        assert 'No messages purged' in out.getvalue()

        a._purge.return_value = 100
        a.run(force=True)
        assert '100 messages' in out.getvalue()

        a.out = Mock(name='out')
        a.ask = Mock(name='ask')
        a.run(force=False)
        a.ask.assert_called_with(a.warn_prompt, ('yes', 'no'), 'no')
        a.ask.return_value = 'yes'
        a.run(force=False)
