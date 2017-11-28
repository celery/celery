# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import os
import tempfile

import pytest
from case import skip

from celery import states, uuid
from celery.backends.filesystem import FilesystemBackend
from celery.exceptions import ImproperlyConfigured


@skip.if_win32()
class test_FilesystemBackend:

    def setup(self):
        self.directory = tempfile.mkdtemp()
        self.url = 'file://' + self.directory
        self.path = self.directory.encode('ascii')

    def test_a_path_is_required(self):
        with pytest.raises(ImproperlyConfigured):
            FilesystemBackend(app=self.app)

    def test_a_path_in_url(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        assert tb.path == self.path

    def test_path_is_incorrect(self):
        with pytest.raises(ImproperlyConfigured):
            FilesystemBackend(app=self.app, url=self.url + '-incorrect')

    def test_missing_task_is_PENDING(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        assert tb.get_state('xxx-does-not-exist') == states.PENDING

    def test_mark_as_done_writes_file(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        tb.mark_as_done(uuid(), 42)
        assert len(os.listdir(self.directory)) == 1

    def test_done_task_is_SUCCESS(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        tid = uuid()
        tb.mark_as_done(tid, 42)
        assert tb.get_state(tid) == states.SUCCESS

    def test_correct_result(self):
        data = {'foo': 'bar'}

        tb = FilesystemBackend(app=self.app, url=self.url)
        tid = uuid()
        tb.mark_as_done(tid, data)
        assert tb.get_result(tid) == data

    def test_get_many(self):
        data = {uuid(): 'foo', uuid(): 'bar', uuid(): 'baz'}

        tb = FilesystemBackend(app=self.app, url=self.url)
        for key, value in data.items():
            tb.mark_as_done(key, value)

        for key, result in tb.get_many(data.keys()):
            assert result['result'] == data[key]

    def test_forget_deletes_file(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        tid = uuid()
        tb.mark_as_done(tid, 42)
        tb.forget(tid)
        assert len(os.listdir(self.directory)) == 0
