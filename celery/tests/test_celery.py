import unittest
import celery

class TestInitFile(unittest.TestCase):

    def test_version(self):
        self.assertTrue(celery.VERSION)
        self.assertEquals(len(celery.VERSION), 3)
        is_stable = not (celery.VERSION[1] % 2)
        self.assertTrue(celery.is_stable_release() == is_stable)
        self.assertEquals(celery.__version__.count("."), 2)
        self.assertTrue("(%s)" % (is_stable and "stable" or "unstable") in \
                celery.version_with_meta())

    def test_meta(self):
        for m in ("__author__", "__contact__", "__homepage__",
                "__docformat__"):
            self.assertTrue(getattr(celery, m, None))
