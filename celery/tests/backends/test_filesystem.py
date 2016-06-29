# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import os
import shutil
import tempfile

from celery import uuid
from celery import states
from celery.backends.filesystem import FilesystemBackend
from celery.exceptions import ImproperlyConfigured

from celery.tests.case import AppCase, skip


@skip.if_win32()
class test_FilesystemBackend(AppCase):

    def setup(self):
        self.directory = tempfile.mkdtemp()
        self.url = 'file://' + self.directory
        self.path = self.directory.encode('ascii')

    def teardown(self):
        shutil.rmtree(self.directory)

    def test_a_path_is_required(self):
        with self.assertRaises(ImproperlyConfigured):
            FilesystemBackend(app=self.app)

    def test_a_path_in_url(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        self.assertEqual(tb.path, self.path)

    def test_path_is_incorrect(self):
        with self.assertRaises(ImproperlyConfigured):
            FilesystemBackend(app=self.app, url=self.url + '-incorrect')

    def test_missing_task_is_PENDING(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        self.assertEqual(tb.get_state('xxx-does-not-exist'), states.PENDING)

    def test_mark_as_done_writes_file(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        tb.mark_as_done(uuid(), 42)
        self.assertEqual(len(os.listdir(self.directory)), 1)

    def test_done_task_is_SUCCESS(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        tid = uuid()
        tb.mark_as_done(tid, 42)
        self.assertEqual(tb.get_state(tid), states.SUCCESS)

    def test_correct_result(self):
        data = {'foo': 'bar'}

        tb = FilesystemBackend(app=self.app, url=self.url)
        tid = uuid()
        tb.mark_as_done(tid, data)
        self.assertEqual(tb.get_result(tid), data)

    def test_get_many(self):
        data = {uuid(): 'foo', uuid(): 'bar', uuid(): 'baz'}

        tb = FilesystemBackend(app=self.app, url=self.url)
        for key, value in data.items():
            tb.mark_as_done(key, value)

        for key, result in tb.get_many(data.keys()):
            self.assertEqual(result['result'], data[key])

    def test_forget_deletes_file(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        tid = uuid()
        tb.mark_as_done(tid, 42)
        tb.forget(tid)
        self.assertEqual(len(os.listdir(self.directory)), 0)
