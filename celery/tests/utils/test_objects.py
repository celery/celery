from __future__ import absolute_import, unicode_literals

from celery.utils.objects import Bunch

from celery.tests.case import Case


class test_Bunch(Case):

    def test(self):
        x = Bunch(foo='foo', bar=2)
        self.assertEqual(x.foo, 'foo')
        self.assertEqual(x.bar, 2)
