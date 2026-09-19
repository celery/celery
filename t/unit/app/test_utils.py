from collections.abc import Mapping, MutableMapping
from copy import copy
from unittest.mock import Mock

import pytest

from celery.app.defaults import Option
from celery.app.utils import Settings, bugreport, filter_hidden_settings


class test_Settings:

    @pytest.mark.parametrize('copy_fun', [lambda settings: settings.copy(), copy])
    @pytest.mark.parametrize('configured', [False, True])
    @pytest.mark.parametrize('namespace, config', [
        (None, {'task_always_eager': True}),
        ('CELERY', {'CELERY_TASK_ALWAYS_EAGER': True}),
        (None, {'CELERY_ALWAYS_EAGER': True}),
    ])
    def test_copy_app_settings(self, copy_fun, configured, namespace, config):
        with self.Celery(set_as_current=False) as app:
            app.config_from_object(config, namespace=namespace)
            app.add_defaults(lambda: {'copy_default': 'custom'})
            if configured:
                assert app.conf.task_always_eager is True
            assert app.configured is configured

            copied = copy_fun(app.conf)

            assert copied.task_always_eager is True
            assert copied.copy_default == 'custom'
            assert dict(copied) == dict(app.conf)
            copied.task_always_eager = False
            assert copied.task_always_eager is False
            assert app.conf.task_always_eager is True

    @pytest.mark.parametrize('copy_fun', [lambda settings: settings.copy(), copy])
    @pytest.mark.parametrize('cleared', [False, True])
    def test_copy_preserves_deprecated_settings(self, copy_fun, cleared):
        settings = Settings(
            {'task_always_eager': True},
            deprecated_settings={'CELERY_ALWAYS_EAGER'},
        )
        if cleared:
            settings.clear()

        copied = copy_fun(settings)

        assert dict(copied) == dict(settings)
        if not cleared:
            assert copied.deprecated_settings is settings.deprecated_settings

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


class test_bugreport:

    def test_no_conn_driver_info(self):
        self.app.connection = Mock()
        conn = self.app.connection.return_value = Mock()
        conn.transport = None

        bugreport(self.app)
