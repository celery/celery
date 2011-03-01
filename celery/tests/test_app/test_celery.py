from celery.tests.utils import unittest

import celery


class TestInitFile(unittest.TestCase):

    def test_version(self):
        self.assertTrue(celery.VERSION)
        self.assertGreaterEqual(len(celery.VERSION), 3)
        celery.VERSION = (0, 3, 0)
        self.assertGreaterEqual(celery.__version__.count("."), 2)

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__",
                "__docformat__"):
            self.assertTrue(getattr(celery, m, None))
