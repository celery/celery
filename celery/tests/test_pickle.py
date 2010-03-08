import unittest

from billiard.serialization import pickle


class RegularException(Exception):
    pass


class ArgOverrideException(Exception):

    def __init__(self, message, status_code=10):
        self.status_code = status_code
        Exception.__init__(self, message, status_code)


class TestPickle(unittest.TestCase):

    def test_pickle_regular_exception(self):
        e = None
        try:
            raise RegularException("RegularException raised")
        except RegularException, e:
            pass

        pickled = pickle.dumps({"exception": e})
        unpickled = pickle.loads(pickled)
        exception = unpickled.get("exception")
        self.assertTrue(exception)
        self.assertTrue(isinstance(exception, RegularException))
        self.assertEquals(exception.args, ("RegularException raised", ))

    def test_pickle_arg_override_exception(self):

        e = None
        try:
            raise ArgOverrideException("ArgOverrideException raised",
                    status_code=100)
        except ArgOverrideException, e:
            pass

        pickled = pickle.dumps({"exception": e})
        unpickled = pickle.loads(pickled)
        exception = unpickled.get("exception")
        self.assertTrue(exception)
        self.assertTrue(isinstance(exception, ArgOverrideException))
        self.assertEquals(exception.args, ("ArgOverrideException raised",
                                          100))
        self.assertEquals(exception.status_code, 100)
