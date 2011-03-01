from celery.tests.utils import unittest

from celery.utils.serialization import pickle


class RegularException(Exception):
    pass


class ArgOverrideException(Exception):

    def __init__(self, message, status_code=10):
        self.status_code = status_code
        Exception.__init__(self, message, status_code)


class TestPickle(unittest.TestCase):

    def test_pickle_regular_exception(self):
        exc = None
        try:
            raise RegularException("RegularException raised")
        except RegularException, exc_:
            exc = exc_

        pickled = pickle.dumps({"exception": exc})
        unpickled = pickle.loads(pickled)
        exception = unpickled.get("exception")
        self.assertTrue(exception)
        self.assertIsInstance(exception, RegularException)
        self.assertTupleEqual(exception.args, ("RegularException raised", ))

    def test_pickle_arg_override_exception(self):

        exc = None
        try:
            raise ArgOverrideException("ArgOverrideException raised",
                    status_code=100)
        except ArgOverrideException, exc_:
            exc = exc_

        pickled = pickle.dumps({"exception": exc})
        unpickled = pickle.loads(pickled)
        exception = unpickled.get("exception")
        self.assertTrue(exception)
        self.assertIsInstance(exception, ArgOverrideException)
        self.assertTupleEqual(exception.args, (
                              "ArgOverrideException raised", 100))
        self.assertEqual(exception.status_code, 100)
