from __future__ import absolute_import, unicode_literals

from celery.worker import state


class test_revoked:

    def test_is_working(self):
        state.revoked.add('foo')
        assert 'foo' in state.revoked
        state.revoked.pop_value('foo')
        assert 'foo' not in state.revoked
