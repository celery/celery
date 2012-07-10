from __future__ import absolute_import

from mock import patch

from celery import platforms
from celery.tests.utils import unittest


class test_drop_privileges(unittest.TestCase):

    @patch("os.setuid")
    @patch("os.setgid")
    @patch("celery.platforms.parse_uid")
    @patch("celery.platforms.parse_gid")
    def test_uid_gid_is_set(self, parse_gid, parse_uid, setgid, setuid):
        parse_gid.return_value = 20
        parse_uid.return_value = 20

        platforms.set_effective_user("foo", "foo")

        setuid.assert_called_with(20)
        setgid.assert_called_with(20)
