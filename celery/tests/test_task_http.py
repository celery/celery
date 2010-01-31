# -*- coding: utf-8 -*-
import unittest

from celery.task import http


class TestEncodings(unittest.TestCase):

    def test_utf8dict(self):
        d = {u"følelser ær langé": "ærbadægzaååÆØÅ",
              "foobar": "xuzzybaz"}

        for key, value in http.utf8dict(d.items()).items():
            self.assertTrue(isinstance(key, str))
            self.assertTrue(isinstance(value, str))

