from __future__ import absolute_import
from __future__ import with_statement

import sys

from celery.tests.utils import Case, mask_modules


class TestAAPickle(Case):

    def test_no_cpickle(self):
        prev = sys.modules.pop("celery.utils.serialization", None)
        try:
            with mask_modules("cPickle"):
                from celery.utils.serialization import pickle
                import pickle as orig_pickle
                self.assertIs(pickle.dumps, orig_pickle.dumps)
        finally:
            sys.modules["celery.utils.serialization"] = prev
