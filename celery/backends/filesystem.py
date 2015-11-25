# -*- coding: utf-8 -*-
"""
    celery.backends.filesystem
    ~~~~~~~~~~~~~~~~~~~~~~~~~~

    Filesystem result store backend.
"""
from __future__ import absolute_import

from kombu.utils.encoding import ensure_bytes

from celery.exceptions import ImproperlyConfigured
from celery.backends.base import KeyValueStoreBackend
from celery.utils import uuid

import os
import locale
default_encoding = locale.getpreferredencoding(False)

# Python 2 does not have FileNotFoundError and IsADirectoryError
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError
    IsADirectoryError = IOError

E_PATH_INVALID = """\
The configured path for the Filesystem backend does not
work correctly, please make sure that it exists and has
the correct permissions.\
"""


class FilesystemBackend(KeyValueStoreBackend):

    def __init__(self, url=None, open=open, unlink=os.unlink, sep=os.sep,
                 encoding=default_encoding, *args, **kwargs):
        """Initialize the filesystem backend.

        Keyword arguments (in addition to those of KeyValueStoreBackend):

        :param url:  URL to the directory we should use
        :param open: open function to use when opening files
        :param unlink: unlink function to use when deleting files
        :param sep: directory seperator (to join the directory with the key)
        :param encoding: encoding used on the filesystem

        """

        super(FilesystemBackend, self).__init__(*args, **kwargs)
        path = self._find_path(url)

        # We need the path and seperator as bytes objects
        self.path = path.encode(encoding)
        self.sep = sep.encode(encoding)

        self.open = open
        self.unlink = unlink

        # Lets verify that we have everything setup right
        self._do_directory_test(b'.fs-backend-' + uuid().encode(encoding))

    def _find_path(self, url):
        if url is not None and url.startswith('file:///'):
            return url[7:]
        path = self.app.conf.result_fspath
        if not path:
            raise ImproperlyConfigured(
                'You need to configure a path for the Filesystem backend')
        return path

    def _do_directory_test(self, key):
        try:
            self.set(key, b'test value')
            assert self.get(key) == b'test value'
            self.delete(key)
        except IOError:
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
        with self.open(self._filename(key), 'wb') as outfile:
            outfile.write(ensure_bytes(value))

    def mget(self, keys):
        for key in keys:
            yield self.get(key)

    def delete(self, key):
        self.unlink(self._filename(key))
