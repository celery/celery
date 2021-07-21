import os
import pickle
import tempfile

import pytest

import t.skip
from celery import states, uuid
from celery.backends import filesystem
from celery.backends.filesystem import FilesystemBackend
from celery.exceptions import ImproperlyConfigured


@t.skip.if_win32
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

    @pytest.mark.parametrize("url,expected_error_message", [
        ('file:///non-existing', filesystem.E_PATH_INVALID),
        ('url://non-conforming', filesystem.E_PATH_NON_CONFORMING_SCHEME),
        (None, filesystem.E_NO_PATH_SET)
    ])
    def test_raises_meaningful_errors_for_invalid_urls(
        self,
        url,
        expected_error_message
    ):
        with pytest.raises(
            ImproperlyConfigured,
            match=expected_error_message
        ):
            FilesystemBackend(app=self.app, url=url)

    def test_localhost_is_removed_from_url(self):
        url = 'file://localhost' + self.directory
        tb = FilesystemBackend(app=self.app, url=url)
        assert tb.path == self.path

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

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_pickleable(self):
        tb = FilesystemBackend(app=self.app, url=self.url, serializer='pickle')
        assert pickle.loads(pickle.dumps(tb))
