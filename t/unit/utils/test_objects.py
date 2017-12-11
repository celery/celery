<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from celery.utils.objects import Bunch


class test_Bunch:

    def test(self):
        x = Bunch(foo='foo', bar=2)
        assert x.foo == 'foo'
        assert x.bar == 2
