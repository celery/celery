from collections.abc import Mapping, MutableMapping
from unittest.mock import Mock

from celery.app.utils import Settings, bugreport, filter_hidden_settings, get_broker_connection_retry_configuration


class test_Settings:

    def test_is_mapping(self):
        """Settings should be a collections.Mapping"""
        assert issubclass(Settings, Mapping)

    def test_is_mutable_mapping(self):
        """Settings should be a collections.MutableMapping"""
        assert issubclass(Settings, MutableMapping)

    def test_find(self):
        assert self.app.conf.find_option('always_eager')

    def test_get_by_parts(self):
        self.app.conf.task_do_this_and_that = 303
        assert self.app.conf.get_by_parts(
            'task', 'do', 'this', 'and', 'that') == 303

    def test_find_value_for_key(self):
        assert self.app.conf.find_value_for_key(
            'always_eager') is False

    def test_table(self):
        assert self.app.conf.table(with_defaults=True)
        assert self.app.conf.table(with_defaults=False)
        assert self.app.conf.table(censored=False)
        assert self.app.conf.table(censored=True)


class test_filter_hidden_settings:

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


class test_bugreport:

    def test_no_conn_driver_info(self):
        self.app.connection = Mock()
        conn = self.app.connection.return_value = Mock()
        conn.transport = None

        bugreport(self.app)


class test_get_broker_connection_retry_configuration:

    def test_startup_uses_broker_connection_retry_on_startup(self):
        self.app.conf.broker_connection_retry = False
        self.app.conf.broker_connection_retry_on_startup = True

        retry_enabled, setting_name, warning = (
            get_broker_connection_retry_configuration(
                self.app.conf, first_connection_attempt=True,
            )
        )

        assert retry_enabled is True
        assert setting_name == 'broker_connection_retry_on_startup'
        assert warning is None

    def test_non_startup_uses_broker_connection_retry(self):
        self.app.conf.broker_connection_retry = False
        self.app.conf.broker_connection_retry_on_startup = True

        retry_enabled, setting_name, warning = (
            get_broker_connection_retry_configuration(
                self.app.conf, first_connection_attempt=False,
            )
        )

        assert retry_enabled is False
        assert setting_name == 'broker_connection_retry'
        assert warning is None

    def test_startup_warns_when_retry_on_startup_is_unset_and_retry_disabled(self):
        self.app.conf.broker_connection_retry = False
        self.app.conf.broker_connection_retry_on_startup = None

        retry_enabled, setting_name, warning = (
            get_broker_connection_retry_configuration(
                self.app.conf, first_connection_attempt=True,
            )
        )

        assert retry_enabled is False
        assert setting_name == 'broker_connection_retry'
        assert warning is not None
