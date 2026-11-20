"""File-system result store backend."""
import locale
import os
import stat
import time
from datetime import datetime

from kombu.utils.encoding import ensure_bytes

from celery import uuid
from celery.backends.base import KeyValueStoreBackend
from celery.exceptions import ImproperlyConfigured

default_encoding = locale.getpreferredencoding(False)

E_NO_PATH_SET = 'You need to configure a path for the file-system backend'
E_PATH_NON_CONFORMING_SCHEME = (
    'A path for the file-system backend should conform to the file URI scheme'
)
E_PATH_INVALID = """\
The configured path for the file-system backend does not
work correctly, please make sure that it exists and has
the correct permissions.\
"""


class FilesystemBackend(KeyValueStoreBackend):
    """File-system result backend.

    Arguments:
        url (str):  URL to the directory we should use
        open (Callable): open function to use when opening files
        unlink (Callable): unlink function to use when deleting files
        sep (str): directory separator (to join the directory with the key)
        encoding (str): encoding used on the file-system
    """

    # Result files are opened in binary mode in both directions.
    supports_result_compression = True

    def __init__(self, url=None, open=open, unlink=os.unlink, sep=os.sep,
                 encoding=default_encoding, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.url = url
        path = self._find_path(url)

        # Remove forwarding "/" for Windows os
        if os.name == "nt" and path.startswith("/"):
            path = path[1:]

        # We need the path and separator as bytes objects
        self.path = path.encode(encoding)
        self.sep = sep.encode(encoding)

        self.open = open
        self.unlink = unlink

        # Let's verify that we've everything setup right
        self._do_directory_test(b'.fs-backend-' + uuid().encode(encoding))

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return super().__reduce__(args, {**kwargs, 'url': self.url})

    def _find_path(self, url):
        if not url:
            raise ImproperlyConfigured(E_NO_PATH_SET)
        if url.startswith('file://localhost/'):
            return url[16:]
        if url.startswith('file://'):
            return url[7:]
        raise ImproperlyConfigured(E_PATH_NON_CONFORMING_SCHEME)

    def _do_directory_test(self, key):
        try:
            self.set(key, b'test value')
            assert self.get(key) == b'test value'
            self.delete(key)
        except OSError:
            raise ImproperlyConfigured(E_PATH_INVALID)

    def _filename(self, key):
        return self.sep.join((self.path, key))

    def get(self, key):
        try:
            with self.open(self._filename(key), 'rb') as infile:
                return infile.read()
        except FileNotFoundError:
            pass

    def set(self, key, value):
        filename = self._filename(key)
        # Write to a temporary file in the same directory and rename it into
        # place so concurrent readers (e.g. get/get_many polling the same
        # key) never observe a truncated file. The temp file is created with
        # the umask-derived default mode and an existing result file's mode
        # is preserved across overwrites, matching plain open('wb'). The
        # rename replaces the destination instead of writing through it: a
        # symlink (or different ownership/ACLs) on the result path is not
        # followed, and the link target is left untouched.
        hexsuffix = uuid()
        if isinstance(filename, bytes):
            tmp_name = os.path.join(
                os.path.dirname(filename), b'celery-result-' + hexsuffix.encode() + b'.tmp')
        else:
            tmp_name = os.path.join(
                os.path.dirname(filename), f'celery-result-{hexsuffix}.tmp')
        try:
            with self.open(tmp_name, 'wb') as outfile:
                outfile.write(ensure_bytes(value))
            try:
                os.chmod(tmp_name, stat.S_IMODE(os.stat(filename).st_mode))
            except FileNotFoundError:
                pass  # new file: keep the umask-derived default mode
            # On Windows a reader holding the destination open can make
            # os.replace() fail with PermissionError (CPython opens files
            # without FILE_SHARE_DELETE); retry briefly so a concurrent
            # read doesn't turn a harmless race into a failed store_result.
            for attempt in range(5):
                try:
                    os.replace(tmp_name, filename)
                    break
                except PermissionError:
                    if attempt == 4:
                        raise
                    time.sleep(0.01)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise

    def mget(self, keys):
        for key in keys:
            yield self.get(key)

    def delete(self, key):
        self.unlink(self._filename(key))

    def cleanup(self):
        """Delete expired meta-data."""
        if not self.expires:
            return
        epoch = datetime(1970, 1, 1, tzinfo=self.app.timezone)
        now_ts = (self.app.now() - epoch).total_seconds()
        cutoff_ts = now_ts - self.expires
        for filename in os.listdir(self.path):
            path = os.path.join(self.path, filename)
            # Orphaned temp files (a process killed between the write and
            # the os.replace() in set()) carry no task key prefix, so they
            # would survive cleanup() forever; drop them once stale too.
            if filename.startswith(b'celery-result-') and filename.endswith(b'.tmp'):
                try:
                    if os.stat(path).st_mtime < cutoff_ts:
                        self.unlink(path)
                except FileNotFoundError:
                    pass  # another worker removed it first
                continue
            for prefix in (self.task_keyprefix, self.group_keyprefix,
                           self.chord_keyprefix):
                if filename.startswith(prefix):
                    try:
                        if os.stat(path).st_mtime < cutoff_ts:
                            self.unlink(path)
                    except FileNotFoundError:
                        pass  # another worker removed it first
                    break
