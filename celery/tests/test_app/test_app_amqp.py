from celery.tests.utils import unittest

from celery.app.amqp import MSG_OPTIONS, extract_msg_options


class TestMsgOptions(unittest.TestCase):

    def test_MSG_OPTIONS(self):
        self.assertTrue(MSG_OPTIONS)

    def test_extract_msg_options(self):
        testing = {"mandatory": True, "routing_key": "foo.xuzzy"}
        result = extract_msg_options(testing)
        self.assertEqual(result["mandatory"], True)
        self.assertEqual(result["routing_key"], "foo.xuzzy")
