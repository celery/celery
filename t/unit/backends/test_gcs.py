from datetime import datetime
from unittest.mock import Mock, call, patch

import pytest
from google.cloud.exceptions import NotFound

from celery.backends.gcs import GCSBackend
from celery.exceptions import ImproperlyConfigured


class test_GCSBackend:
    def setup_method(self):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'

    @pytest.fixture(params=['', 'test_folder/'])
    def base_path(self, request):
        return request.param

    @pytest.fixture(params=[86400, None])
    def ttl(self, request):
        return request.param

    def test_missing_storage_module(self):
        with patch('celery.backends.gcs.storage', None):
            with pytest.raises(ImproperlyConfigured, match='You must install'):
                GCSBackend(app=self.app)

    def test_missing_bucket(self):
        self.app.conf.gcs_bucket = None

        with pytest.raises(ImproperlyConfigured, match='Missing bucket name'):
            GCSBackend(app=self.app)

    def test_missing_project(self):
        self.app.conf.gcs_project = None

        with pytest.raises(ImproperlyConfigured, match='Missing project'):
            GCSBackend(app=self.app)

    def test_invalid_ttl(self):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'
        self.app.conf.gcs_ttl = -1

        with pytest.raises(ImproperlyConfigured, match='Invalid ttl'):
            GCSBackend(app=self.app)

    @patch.object(GCSBackend, '_is_bucket_lifecycle_rule_exists')
    def test_ttl_missing_lifecycle_rule(self, mock_lifecycle):
        self.app.conf.gcs_ttl = 86400

        mock_lifecycle.return_value = False
        with pytest.raises(
            ImproperlyConfigured, match='Missing lifecycle rule'
        ):
            GCSBackend(app=self.app)
            mock_lifecycle.assert_called_once()

    @patch.object(GCSBackend, '_get_blob')
    def test_get_key(self, mock_get_blob, base_path):
        self.app.conf.gcs_base_path = base_path

        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        backend = GCSBackend(app=self.app)
        backend.get(b"testkey1")

        mock_get_blob.assert_called_once_with('testkey1')
        mock_blob.download_as_bytes.assert_called_once()

    @patch.object(GCSBackend, 'bucket')
    @patch.object(GCSBackend, '_get_blob')
    def test_set_key(self, mock_get_blob, mock_bucket_prop, base_path, ttl):
        self.app.conf.gcs_base_path = base_path
        self.app.conf.gcs_ttl = ttl

        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        mock_bucket_prop.lifecycle_rules = [{'action': {'type': 'Delete'}}]
        backend = GCSBackend(app=self.app)
        backend.set('testkey', 'testvalue')
        mock_get_blob.assert_called_once_with('testkey')
        mock_blob.upload_from_string.assert_called_once_with(
            'testvalue', retry=backend._retry_policy
        )
        if ttl:
            assert mock_blob.custom_time >= datetime.utcnow()

    @patch.object(GCSBackend, '_get_blob')
    def test_get_missing_key(self, mock_get_blob):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'

        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob

        mock_blob.download_as_bytes.side_effect = NotFound('not found')
        gcs_backend = GCSBackend(app=self.app)
        result = gcs_backend.get('some-key')

        assert result is None

    @patch.object(GCSBackend, '_get_blob')
    def test_delete_existing_key(self, mock_get_blob, base_path):
        self.app.conf.gcs_base_path = base_path

        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        mock_blob.exists.return_value = True
        backend = GCSBackend(app=self.app)
        backend.delete(b"testkey2")

        mock_get_blob.assert_called_once_with('testkey2')
        mock_blob.exists.assert_called_once()
        mock_blob.delete.assert_called_once()

    @patch.object(GCSBackend, '_get_blob')
    def test_delete_missing_key(self, mock_get_blob, base_path):
        self.app.conf.gcs_base_path = base_path

        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        mock_blob.exists.return_value = False
        backend = GCSBackend(app=self.app)
        backend.delete(b"testkey2")

        mock_get_blob.assert_called_once_with('testkey2')
        mock_blob.exists.assert_called_once()
        mock_blob.delete.assert_not_called()

    @patch.object(GCSBackend, 'get')
    def test_mget(self, mock_get, base_path):
        self.app.conf.gcs_base_path = base_path
        backend = GCSBackend(app=self.app)
        mock_get.side_effect = ['value1', 'value2']
        result = backend.mget([b'key1', b'key2'])
        mock_get.assert_has_calls([call(b'key1'), call(b'key2')])
        assert result == ['value1', 'value2']

    @patch('celery.backends.gcs.Client')
    @patch('celery.backends.gcs.getpid')
    def test_new_client_after_fork(self, mock_pid, mock_client):
        mock_pid.return_value = 123
        backend = GCSBackend(app=self.app)
        client1 = backend.client
        mock_pid.assert_called()
        mock_client.assert_called()
        mock_pid.return_value = 456
        mock_client.return_value = Mock()
        assert client1 != backend.client
        mock_client.assert_called_with(project='project')
