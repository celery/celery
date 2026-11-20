import os
import pickle
import sys
import tempfile
import time
from unittest.mock import patch

import pytest

import t.skip
from celery import states, uuid
from celery.backends import filesystem
from celery.backends.base import COMPRESSED_PAYLOAD_MAGIC
from celery.backends.filesystem import FilesystemBackend
from celery.exceptions import ImproperlyConfigured


@t.skip.if_win32
class test_FilesystemBackend:

    def setup_method(self):
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

    def test_compressed_result_is_written_compressed(self):
        data = {'foo': 'bar' * 100}

        self.app.conf.result_compression = 'gzip'
        tb = FilesystemBackend(app=self.app, url=self.url)
        assert tb.compression == 'gzip'
        tid = uuid()
        tb.mark_as_done(tid, data)

        stored = tb.get(tb.get_key_for_task(tid))
        assert stored.startswith(COMPRESSED_PAYLOAD_MAGIC)
        assert tb.get_result(tid) == data

    def test_result_written_before_compression_is_still_readable(self):
        data = {'foo': 'bar'}
        tid = uuid()
        FilesystemBackend(app=self.app, url=self.url).mark_as_done(tid, data)

        self.app.conf.result_compression = 'gzip'
        tb = FilesystemBackend(app=self.app, url=self.url)
        assert tb.get_result(tid) == data

    def test_compressed_binary_serializer_result_round_trips(self):
        # pickle's payload is bytes before compression as well as after it,
        # and the value here has no valid text encoding, so nothing on the
        # way to disk and back can treat the payload as a string.
        data = {'value': b'\x00\x01\x02\xff', 'text': 'a value ' * 40}
        self.app.conf.result_serializer = 'pickle'
        self.app.conf.accept_content = ['pickle']
        self.app.conf.result_compression = 'gzip'

        tb = FilesystemBackend(app=self.app, url=self.url)
        tid = uuid()
        tb.mark_as_done(tid, data)

        with open(os.path.join(self.directory,
                               tb.get_key_for_task(tid).decode()), 'rb') as f:
            on_disk = f.read()
        assert on_disk.startswith(COMPRESSED_PAYLOAD_MAGIC)
        assert tb.get_result(tid) == data

    def test_binary_serializer_result_is_not_read_as_compressed(self):
        # A pickle payload written with compression off must not be mistaken
        # for a compressed one by a reader that has compression on.
        data = {'value': b'\x00\x01\x02\xff'}
        self.app.conf.result_serializer = 'pickle'
        self.app.conf.accept_content = ['pickle']
        tid = uuid()
        FilesystemBackend(app=self.app, url=self.url).mark_as_done(tid, data)

        self.app.conf.result_compression = 'gzip'
        tb = FilesystemBackend(app=self.app, url=self.url)
        assert tb.get_result(tid) == data

    def test_compressed_result_is_readable_with_compression_off(self):
        data = {'foo': 'bar'}
        tid = uuid()
        self.app.conf.result_compression = 'gzip'
        FilesystemBackend(app=self.app, url=self.url).mark_as_done(tid, data)

        self.app.conf.result_compression = None
        tb = FilesystemBackend(app=self.app, url=self.url)
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

    def test_forget_missing_task_is_noop(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        # other KV backends tolerate deleting an absent key; forget()
        # on a result that was expired, already forgotten or never stored
        # must not raise FileNotFoundError
        tb.forget(uuid())

    def test_forget_twice_is_noop(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        tid = uuid()
        tb.mark_as_done(tid, 42)
        tb.forget(tid)
        tb.forget(tid)

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_pickleable(self):
        tb = FilesystemBackend(app=self.app, url=self.url, serializer='pickle')
        assert pickle.loads(pickle.dumps(tb))

    @pytest.mark.skipif(sys.platform == 'win32', reason='Test can fail on '
                        'Windows/FAT due to low granularity of st_mtime')
    def test_cleanup_skips_files_removed_concurrently(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        tids = [uuid() for _ in range(4)]
        for tid in tids:
            tb.mark_as_done(tid, 42)
        # remove one file behind cleanup()'s back, between listdir and stat
        victim = os.path.join(tb.path, tb.get_key_for_task(tids[1]))
        original_stat = os.stat

        def racy_stat(path, *args, **kwargs):
            result = original_stat(path, *args, **kwargs)
            if os.path.abspath(path) == os.path.abspath(victim):
                os.unlink(victim)
            return result

        with patch.object(tb, 'expires', 10), patch('os.stat', side_effect=racy_stat):
            tb.cleanup()  # must not raise FileNotFoundError
        assert not os.path.exists(victim)
        for tid in tids[2:]:
            assert os.path.exists(os.path.join(tb.path, tb.get_key_for_task(tid)))

    @pytest.mark.skipif(sys.platform == 'win32', reason='Test can fail on '
                        'Windows/FAT due to low granularity of st_mtime')
    def test_cleanup_skips_files_vanishing_before_unlink(self):
        # the other interleaving: the file disappears between cleanup()'s
        # stat() and unlink() — the unlink's own FileNotFoundError guard
        tb = FilesystemBackend(app=self.app, url=self.url)
        tids = [uuid() for _ in range(4)]
        for tid in tids:
            tb.mark_as_done(tid, 42)
        victim = os.path.join(tb.path, tb.get_key_for_task(tids[1]))
        # age only the victim past expires so cleanup() unlinks it while
        # the other (fresh) files must survive untouched
        stale = time.time() - 3600
        os.utime(victim, (stale, stale))
        original_unlink = os.unlink

        def racy_unlink(path, *args, **kwargs):
            if os.path.abspath(path) == os.path.abspath(victim):
                os.unlink(victim)  # gone before the real unlink runs
                raise FileNotFoundError(victim)
            return original_unlink(path, *args, **kwargs)

        with patch.object(tb, 'expires', 10), \
                patch('os.unlink', side_effect=racy_unlink):
            tb.cleanup()  # must not raise FileNotFoundError
        assert not os.path.exists(victim)
        for tid in tids[2:]:
            assert os.path.exists(os.path.join(tb.path, tb.get_key_for_task(tid)))

    def test_cleanup(self):
        tb = FilesystemBackend(app=self.app, url=self.url)
        yesterday_task_ids = [uuid() for i in range(10)]
        today_task_ids = [uuid() for i in range(10)]
        for tid in yesterday_task_ids:
            tb.mark_as_done(tid, 42)
        day_length = 0.2
        time.sleep(day_length)  # let FS mark some difference in mtimes
        for tid in today_task_ids:
            tb.mark_as_done(tid, 42)
        with patch.object(tb, 'expires', 0):
            tb.cleanup()
        # test that zero expiration time prevents any cleanup
        filenames = set(os.listdir(tb.path))
        assert all(
            tb.get_key_for_task(tid) in filenames
            for tid in yesterday_task_ids + today_task_ids
        )
        # test that non-zero expiration time enables cleanup by file mtime
        with patch.object(tb, 'expires', day_length):
            tb.cleanup()
        filenames = set(os.listdir(tb.path))
        assert not any(
            tb.get_key_for_task(tid) in filenames
            for tid in yesterday_task_ids
        )
        assert all(
            tb.get_key_for_task(tid) in filenames
            for tid in today_task_ids
        )
