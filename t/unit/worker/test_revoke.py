<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from celery.worker import state


class test_revoked:

    def test_is_working(self):
        state.revoked.add('foo')
        assert 'foo' in state.revoked
        state.revoked.pop_value('foo')
        assert 'foo' not in state.revoked
