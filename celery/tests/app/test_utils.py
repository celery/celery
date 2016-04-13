from __future__ import absolute_import, unicode_literals

from collections import Mapping, MutableMapping

from celery.app.utils import Settings, filter_hidden_settings, bugreport

from celery.tests.case import AppCase, Mock


class test_Settings(AppCase):

    def test_is_mapping(self):
        """Settings should be a collections.Mapping"""
        self.assertTrue(issubclass(Settings, Mapping))

    def test_is_mutable_mapping(self):
        """Settings should be a collections.MutableMapping"""
        self.assertTrue(issubclass(Settings, MutableMapping))

    def test_find(self):
        self.assertTrue(self.app.conf.find_option('always_eager'))

    def test_get_by_parts(self):
        self.app.conf.task_do_this_and_that = 303
        self.assertEqual(
            self.app.conf.get_by_parts('task', 'do', 'this', 'and', 'that'),
            303,
        )

    def test_find_value_for_key(self):
        self.assertEqual(
            self.app.conf.find_value_for_key('always_eager'),
            False,
        )

    def test_table(self):
        self.assertTrue(self.app.conf.table(with_defaults=True))
        self.assertTrue(self.app.conf.table(with_defaults=False))
        self.assertTrue(self.app.conf.table(censored=False))
        self.assertTrue(self.app.conf.table(censored=True))


class test_filter_hidden_settings(AppCase):

    def test_handles_non_string_keys(self):
        """filter_hidden_settings shouldn't raise an exception when handling
        mappings with non-string keys"""
        conf = {
            'STRING_KEY': 'VALUE1',
            ('NON', 'STRING', 'KEY'): 'VALUE2',
            'STRING_KEY2': {
                'STRING_KEY3': 1,
                ('NON', 'STRING', 'KEY', '2'): 2
            },
        }
        filter_hidden_settings(conf)


class test_bugreport(AppCase):

    def test_no_conn_driver_info(self):
        self.app.connection = Mock()
        conn = self.app.connection.return_value = Mock()
        conn.transport = None

        bugreport(self.app)
