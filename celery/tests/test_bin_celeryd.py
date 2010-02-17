import unittest

from celery.bin import celeryd


class TestWorker(unittest.TestCase):

    def test_init_loader(self):

        w = celeryd.Worker()
        w.init_loader()
        self.assertTrue(w.loader)
        self.assertTrue(w.settings)
