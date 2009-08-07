import unittest
from celery.messaging import MSG_OPTIONS, get_msg_options, extract_msg_options


class TestMsgOptions(unittest.TestCase):

    def test_MSG_OPTIONS(self):
        self.assertTrue(MSG_OPTIONS)

    def test_extract_msg_options(self):
        testing = {"mandatory": True, "routing_key": "foo.xuzzy"}
        result = extract_msg_options(testing)
        self.assertEquals(result["mandatory"], True)
        self.assertEquals(result["routing_key"], "foo.xuzzy")
