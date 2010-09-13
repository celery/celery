import sys
import unittest2 as unittest

from celery.tests.utils import execute_context, mask_modules


class TestAAPickle(unittest.TestCase):

    def test_no_cpickle(self):
        prev = sys.modules.pop("celery.serialization", None)
        try:
            def with_cPickle_masked(_val):
                from celery.serialization import pickle
                import pickle as orig_pickle
                self.assertIs(pickle.dumps, orig_pickle.dumps)

            context = mask_modules("cPickle")
            execute_context(context, with_cPickle_masked)

        finally:
            sys.modules["celery.serialization"] = prev
