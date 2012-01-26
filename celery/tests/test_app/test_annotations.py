from __future__ import absolute_import

from celery.app.annotations import MapAnnotation, prepare
from celery.task import task
from celery.utils import qualname

from celery.tests.utils import Case


@task
def add(x, y):
    return x + y


@task
def mul(x, y):
    return x * y


class MyAnnotation(object):
    foo = 65


class test_MapAnnotation(Case):

    def test_annotate(self):
        x = MapAnnotation({add.name: {"foo": 1}})
        self.assertDictEqual(x.annotate(add), {"foo": 1})
        self.assertIsNone(x.annotate(mul))

    def test_annotate_any(self):
        x = MapAnnotation({'*': {"foo": 2}})
        self.assertDictEqual(x.annotate_any(), {"foo": 2})

        x = MapAnnotation()
        self.assertIsNone(x.annotate_any())


class test_prepare(Case):

    def test_dict_to_MapAnnotation(self):
        x = prepare({add.name: {"foo": 3}})
        self.assertIsInstance(x[0], MapAnnotation)

    def test_returns_list(self):
        self.assertListEqual(prepare(1), [1])
        self.assertListEqual(prepare([1]), [1])
        self.assertListEqual(prepare((1, )), [1])
        self.assertEqual(prepare(None), ())

    def test_evalutes_qualnames(self):
        self.assertEqual(prepare(qualname(MyAnnotation))[0]().foo, 65)
        self.assertEqual(prepare([qualname(MyAnnotation)])[0]().foo, 65)
