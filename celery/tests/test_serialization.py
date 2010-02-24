import sys
import unittest

from celery.tests.utils import execute_context


class TestAAPickle(unittest.TestCase):

    def test_no_cpickle(self):
        from celery.tests.utils import mask_modules
        prev = sys.modules.pop("billiard.serialization")
        try:
            def with_cPickle_masked():
                from billiard.serialization import pickle
                import pickle as orig_pickle
                self.assertTrue(pickle.dumps is orig_pickle.dumps)

            context = mask_modules("cPickle")
            execute_context(context, with_cPickle_masked)

        finally:
            sys.modules["billiard.serialization"] = prev
