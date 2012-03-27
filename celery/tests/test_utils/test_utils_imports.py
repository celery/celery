from __future__ import absolute_import

from celery.utils.imports import qualname, symbol_by_name

from celery.tests.utils import Case


class test_import_utils(Case):

    def test_qualname(self):
        Class = type("Fox", (object, ), {"__module__": "quick.brown"})
        self.assertEqual(qualname(Class), "quick.brown.Fox")
        self.assertEqual(qualname(Class()), "quick.brown.Fox")

    def test_symbol_by_name__instance_returns_instance(self):
        instance = object()
        self.assertIs(symbol_by_name(instance), instance)
