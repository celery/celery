from __future__ import absolute_import

from collections import Mapping, MutableMapping

from celery import Celery
from celery.app.utils import Settings, bugreport

from celery.tests.case import AppCase, Case, Mock


class TestSettings(Case):
    """
    Tests of celery.app.utils.Settings
    """
    def test_is_mapping(self):
        """Settings should be a collections.Mapping"""
        self.assertTrue(issubclass(Settings, Mapping))

    def test_is_mutable_mapping(self):
        """Settings should be a collections.MutableMapping"""
        self.assertTrue(issubclass(Settings, MutableMapping))


class test_bugreport(AppCase):

    def test_no_conn_driver_info(self):
        app = Celery(set_as_current=False)
        app.connection = Mock()
        conn = app.connection.return_value = Mock()
        conn.transport = None

        bugreport(app)
