from __future__ import absolute_import, unicode_literals

import pytest

from case import MagicMock, Mock, sentinel, skip

from celery.backends import couchdb as module
from celery.backends.couchdb import CouchBackend
from celery.exceptions import ImproperlyConfigured
from celery import backends

try:
    import pycouchdb
except ImportError:
    pycouchdb = None  # noqa

COUCHDB_CONTAINER = 'celery_container'


@skip.unless_module('pycouchdb')
class test_CouchBackend:

    def setup(self):
        self.Server = self.patching('pycouchdb.Server')
        self.backend = CouchBackend(app=self.app)

    def test_init_no_pycouchdb(self):
        """test init no pycouchdb raises"""
        prev, module.pycouchdb = module.pycouchdb, None
        try:
            with pytest.raises(ImproperlyConfigured):
                CouchBackend(app=self.app)
        finally:
            module.pycouchdb = prev

    def test_get_container_exists(self):
            self.backend._connection = sentinel._connection
            connection = self.backend.connection
            assert connection is sentinel._connection
            self.Server.assert_not_called()

    def test_get(self):
        """test_get

        CouchBackend.get should return  and take two params
        db conn to couchdb is mocked.
        TODO Should test on key not exists

        """
        x = CouchBackend(app=self.app)
        x._connection = Mock()
        get = x._connection.get = MagicMock()
        # should return None
        assert x.get('1f3fab') == get.return_value['value']
        x._connection.get.assert_called_once_with('1f3fab')

    def test_delete(self):
        """test_delete

        CouchBackend.delete should return and take two params
        db conn to pycouchdb is mocked.
        TODO Should test on key not exists

        """
        x = CouchBackend(app=self.app)
        x._connection = Mock()
        mocked_delete = x._connection.delete = Mock()
        mocked_delete.return_value = None
        # should return None
        assert x.delete('1f3fab') is None
        x._connection.delete.assert_called_once_with('1f3fab')

    def test_backend_by_url(self, url='couchdb://myhost/mycoolcontainer'):
        from celery.backends.couchdb import CouchBackend
        backend, url_ = backends.get_backend_by_url(url, self.app.loader)
        assert backend is CouchBackend
        assert url_ == url

    def test_backend_params_by_url(self):
        url = 'couchdb://johndoe:mysecret@myhost:123/mycoolcontainer'
        with self.Celery(backend=url) as app:
            x = app.backend
            assert x.container == 'mycoolcontainer'
            assert x.host == 'myhost'
            assert x.username == 'johndoe'
            assert x.password == 'mysecret'
            assert x.port == 123
