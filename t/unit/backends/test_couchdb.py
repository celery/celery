from unittest.mock import MagicMock, Mock, sentinel
from urllib.parse import urlparse

import pytest
from kombu.utils.encoding import str_to_bytes

from celery import states, uuid
from celery.app import backends
from celery.backends import couchdb as module
from celery.backends.couchdb import CouchBackend
from celery.exceptions import ImproperlyConfigured

try:
    import pycouchdb
except ImportError:
    pycouchdb = None

COUCHDB_CONTAINER = 'celery_container'

pytest.importorskip('pycouchdb')


class test_CouchBackend:

    def setup_method(self):
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
        """
        x = CouchBackend(app=self.app)
        x._connection = Mock()
        get = x._connection.get = MagicMock()
        assert x.get('1f3fab') == get.return_value['value']
        x._connection.get.assert_called_once_with('1f3fab')

    def test_get_non_existent_key(self):
        x = CouchBackend(app=self.app)
        x._connection = Mock()
        get = x._connection.get = MagicMock()
        get.side_effect = pycouchdb.exceptions.NotFound
        assert x.get('1f3fab') is None
        x._connection.get.assert_called_once_with('1f3fab')

    @pytest.mark.parametrize("key", ['1f3fab', b'1f3fab'])
    def test_set(self, key):
        x = CouchBackend(app=self.app)
        x._connection = Mock()

        x._set_with_state(key, 'value', states.SUCCESS)

        x._connection.save.assert_called_once_with({'_id': '1f3fab',
                                                    'value': 'value'})

    @pytest.mark.parametrize("key", ['1f3fab', b'1f3fab'])
    def test_set_with_conflict(self, key):
        x = CouchBackend(app=self.app)
        x._connection = Mock()
        x._connection.save.side_effect = (pycouchdb.exceptions.Conflict, None)
        get = x._connection.get = MagicMock()

        x._set_with_state(key, 'value', states.SUCCESS)

        x._connection.get.assert_called_once_with('1f3fab')
        x._connection.get('1f3fab').__setitem__.assert_called_once_with(
            'value', 'value')
        x._connection.save.assert_called_with(get('1f3fab'))
        assert x._connection.save.call_count == 2

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
        backend, url_ = backends.by_url(url, self.app.loader)
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


class CouchSessionMock:
    """
    Mock for `requests.session` that emulates couchdb storage.
    """

    _store = {}

    def request(self, method, url, stream=False, data=None, params=None,
                headers=None, **kw):
        tid = urlparse(url).path.split("/")[-1]

        response = Mock()
        response.headers = {"content-type": "application/json"}
        response.status_code = 200
        response.content = b''

        if method == "GET":
            if tid not in self._store:
                return self._not_found_response()
            response.content = self._store.get(tid)
        elif method == "PUT":
            self._store[tid] = data
            response.content = str_to_bytes(f'{{"ok":true,"id":"{tid}","rev":"1-revid"}}')
        elif method == "HEAD":
            if tid not in self._store:
                return self._not_found_response()
            response.headers.update({"etag": "1-revid"})
        elif method == "DELETE":
            if tid not in self._store:
                return self._not_found_response()
            del self._store[tid]
            response.content = str_to_bytes(f'{{"ok":true,"id":"{tid}","rev":"1-revid"}}')
        else:
            raise NotImplementedError(f"CouchSessionMock.request() does not handle {method} method")

        return response

    def _not_found_response(self):
        response = Mock()
        response.headers = {"content-type": "application/json"}
        response.status_code = 404
        response.content = str_to_bytes('{"error":"not_found","reason":"missing"}')
        return response


class test_CouchBackend_result:
    def setup_method(self):
        self.backend = CouchBackend(app=self.app)
        resource = pycouchdb.resource.Resource("resource-url", session=CouchSessionMock())
        self.backend._connection = pycouchdb.client.Database(resource, "container")

    def test_get_set_forget(self):
        tid = uuid()
        self.backend.store_result(tid, "successful-result", states.SUCCESS)
        assert self.backend.get_state(tid) == states.SUCCESS
        assert self.backend.get_result(tid) == "successful-result"
        self.backend.forget(tid)
        assert self.backend.get_state(tid) == states.PENDING

    def test_mark_as_started(self):
        tid = uuid()
        self.backend.mark_as_started(tid)
        assert self.backend.get_state(tid) == states.STARTED

    def test_mark_as_revoked(self):
        tid = uuid()
        self.backend.mark_as_revoked(tid)
        assert self.backend.get_state(tid) == states.REVOKED

    def test_mark_as_retry(self):
        tid = uuid()
        try:
            raise KeyError('foo')
        except KeyError as exception:
            import traceback
            trace = '\n'.join(traceback.format_stack())
            self.backend.mark_as_retry(tid, exception, traceback=trace)
            assert self.backend.get_state(tid) == states.RETRY
            assert isinstance(self.backend.get_result(tid), KeyError)
            assert self.backend.get_traceback(tid) == trace

    def test_mark_as_failure(self):
        tid = uuid()
        try:
            raise KeyError('foo')
        except KeyError as exception:
            import traceback
            trace = '\n'.join(traceback.format_stack())
            self.backend.mark_as_failure(tid, exception, traceback=trace)
            assert self.backend.get_state(tid) == states.FAILURE
            assert isinstance(self.backend.get_result(tid), KeyError)
            assert self.backend.get_traceback(tid) == trace
