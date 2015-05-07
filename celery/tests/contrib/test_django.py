from __future__ import absolute_import

import datetime
import os
import sys

from celery.tests.case import (
    AppCase, MagicMock, Mock, SkipTest, ANY,
    depends_on_current_app, patch, sentinel,
)
from celery.tests.case import AppCase, patch
from celery.contrib.django import model_task
from celery.tests.app.test_app import test_config
from celery.canvas import group

try:
    import django
    import django.conf
except ImportError:
    django, django.conf = None  # noqa


class test_DjangoModelBaseTask(AppCase):

    @classmethod
    def setUpClass(cls):
        super(test_DjangoModelBaseTask, cls).setUpClass()

        if django is None:
            raise SkipTest('django is not installed.')

        os.environ[django.conf.ENVIRONMENT_VARIABLE] = "celery.tests.contrib.django.settings"

        django.setup()

        from django.test.utils import setup_test_environment
        setup_test_environment()

        from django.db import connection
        connection.creation.create_test_db()

    @classmethod
    def tearDownClass(cls):
        super(test_DjangoModelBaseTask, cls).tearDownClass()

        from django.db import connection
        connection.creation.destroy_test_db("not_needed")

        from django.test.utils import teardown_test_environment
        teardown_test_environment()

    def setup(self):
        self.app.add_defaults(test_config)

    def test_task_apply(self):
        with patch('celery.contrib.django.current_app', self.app):

            @model_task
            def task(self):
                return self.name

            with patch('celery.tests.contrib.django.testapp.models.Foo.task', task, create=True):
                from celery.tests.contrib.django.testapp.models import Foo

                foo = Foo.objects.create(name='foo')
                self.assertEqual(foo.task(), 'foo')

                foo.name = 'bar'
                self.assertEqual(foo.task(), 'bar')

    def test_task_apply_chain(self):
        with patch('celery.contrib.django.current_app', self.app):

            @model_task
            def task(self, bar):
                return bar

            with patch('celery.tests.contrib.django.testapp.models.Foo.task', task, create=True):
                from celery.tests.contrib.django.testapp.models import Foo

                foo = Foo.objects.create(name='foo')
                result = (foo.task.s('bar') | foo.task.s()).apply()
                self.assertEqual(result.get(), 'bar')

    def test_task_apply_group(self):
        with patch('celery.contrib.django.current_app', self.app):

            @model_task
            def task(self, bar):
                return bar

            with patch('celery.tests.contrib.django.testapp.models.Foo.task', task, create=True):
                from celery.tests.contrib.django.testapp.models import Foo

                foo = Foo.objects.create(name='foo')
                result = group(foo.task.s('bar'), foo.task.s('foo')).apply()
                self.assertEqual(result.get(), ['bar', 'foo'])

