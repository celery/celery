from collections.abc import Mapping, MutableMapping
from unittest.mock import Mock

from celery.app.defaults import Option
from celery.app.utils import Settings, bugreport, filter_hidden_settings, sanitize_url


class test_Settings:

    def test_is_mapping(self):
        """Settings should be a collections.Mapping"""
        assert issubclass(Settings, Mapping)

    def test_is_mutable_mapping(self):
        """Settings should be a collections.MutableMapping"""
        assert issubclass(Settings, MutableMapping)

    def test_find(self):
        assert self.app.conf.find_option('always_eager')

    def test_find_option_by_qualified_name(self):
        result = self.app.conf.find_option('task_always_eager')
        assert isinstance(result.type, Option)
        assert result.type.default is False

    def test_get_by_parts(self):
        self.app.conf.task_do_this_and_that = 303
        assert self.app.conf.get_by_parts(
            'task', 'do', 'this', 'and', 'that') == 303

    def test_find_value_for_key(self):
        assert self.app.conf.find_value_for_key(
            'always_eager') is False

    def test_clear_removes_preconfigured_override(self):
        with self.Celery(task_always_eager=True) as app:
            assert app.conf.task_always_eager is True
            app.conf.clear()
            assert app.conf.task_always_eager is False

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

    def test_censors_broker_read_and_write_url(self):
        conf = {
            'broker_url': 'amqp://user:pass@broker.example.com:56721',
            'broker_read_url': 'amqp://user:pass@broker.example.com:56722',
            'broker_write_url': 'amqp://user:pass@broker.example.com:56723',
        }
        censored = filter_hidden_settings(conf)
        assert 'pass' not in censored['broker_url']
        assert 'pass' not in censored['broker_read_url']
        assert 'pass' not in censored['broker_write_url']

    def test_censors_multiserver_backend_url(self):
        conf = {
            'result_backend': (
                'cache+memcached://user:pass1@172.19.26.240:11211;'
                'user:pass2@172.19.26.242:11211/'
            ),
        }
        censored = filter_hidden_settings(conf)
        assert 'pass1' not in censored['result_backend']
        assert 'pass2' not in censored['result_backend']
        assert 'user:********@172.19.26.240:11211' in censored['result_backend']
        assert 'user:********@172.19.26.242:11211' in censored['result_backend']


class test_bugreport:

    def test_no_conn_driver_info(self):
        self.app.connection = Mock()
        conn = self.app.connection.return_value = Mock()
        conn.transport = None

        bugreport(self.app)

    def test_bugreport_with_multiserver_result_backend(self):
        self.app.conf.result_backend = (
            'cache+memcached://172.19.26.240:11211;172.19.26.242:11211/'
        )
        report = bugreport(self.app)
        assert (
            'results:cache+memcached://172.19.26.240:11211;172.19.26.242:11211/'
            in report
        )

    def test_bugreport_with_multiserver_result_backend_passwords(self):
        self.app.conf.result_backend = (
            'cache+memcached://user:secret1@172.19.26.240:11211;'
            'user:secret2@172.19.26.242:11211/'
        )
        report = bugreport(self.app)
        assert 'secret1' not in report
        assert 'secret2' not in report
        assert 'user:********@172.19.26.240:11211' in report
        assert 'user:********@172.19.26.242:11211' in report


class test_sanitize_url:

    def test_non_string_and_empty(self):
        assert sanitize_url(None) is None
        assert sanitize_url('') == ''
        assert sanitize_url(12345) == 12345
        assert sanitize_url([]) == []

    def test_single_server_url(self):
        assert sanitize_url('redis://localhost:6379/0') == 'redis://localhost:6379/0'
        assert sanitize_url('redis://:mypass@localhost:6379/0') == 'redis://:********@localhost:6379/0'
        assert sanitize_url('disabled') == 'disabled'

    def test_multiserver_semicolon(self):
        url = 'cache+memcached://172.19.26.240:11211;172.19.26.242:11211/'
        assert sanitize_url(url) == url

        url_no_slash = 'cache+memcached://172.19.26.240:11211;172.19.26.242:11211'
        assert sanitize_url(url_no_slash) == url_no_slash

        url_with_path = 'cache+memcached://172.19.26.240:11211;172.19.26.242:11211/myprefix?param=1'
        assert sanitize_url(url_with_path) == url_with_path

    def test_multiserver_semicolon_with_passwords(self):
        url = 'cache+memcached://user:pass1@172.19.26.240:11211;user:pass2@172.19.26.242:11211/'
        expected = 'cache+memcached://user:********@172.19.26.240:11211;user:********@172.19.26.242:11211/'
        assert sanitize_url(url) == expected

    def test_multiserver_comma_with_passwords(self):
        url = 'mongodb://user:pass1@host1:27017,user:pass2@host2:27017/dbname?replicaSet=mySet'
        expected = 'mongodb://user:********@host1:27017,user:********@host2:27017/dbname?replicaSet=mySet'
        assert sanitize_url(url) == expected

    def test_multiserver_empty_chunk(self):
        url = 'cache+memcached://172.19.26.240:11211;;172.19.26.242:11211/'
        expected = 'cache+memcached://172.19.26.240:11211;172.19.26.242:11211/'
        assert sanitize_url(url) == expected

    def test_malformed_url_fallback(self):
        url = 'invalid://bad:url:extra:colons'
        assert sanitize_url(url) == url
