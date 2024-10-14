import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from google.cloud.exceptions import NotFound

from celery.exceptions import ImproperlyConfigured

# Workaround until python-firestore is fixed
is_py313 = sys.version_info >= (3, 13)
if not is_py313:
    from celery.backends.gcs import GCSBackend
else:
    GCSBackend = None


@pytest.mark.skipif(
    is_py313,
    reason="https://github.com/googleapis/python-firestore/issues/973",
)
class test_GCSBackend:
    def setup_method(self):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'

    @pytest.fixture(params=['', 'test_folder/'])
    def base_path(self, request):
        return request.param

    @pytest.fixture(params=[86400, None])
    def gcs_ttl(self, request):
        return request.param

    def test_missing_storage_module(self):
        with patch('celery.backends.gcs.storage', None):
            with pytest.raises(
                ImproperlyConfigured, match='You must install'
            ):
                GCSBackend(app=self.app)

    def test_missing_firestore_module(self):
        with patch('celery.backends.gcs.firestore', None):
            with pytest.raises(
                ImproperlyConfigured, match='You must install'
            ):
                GCSBackend(app=self.app)

    def test_missing_bucket(self):
        self.app.conf.gcs_bucket = None

        with pytest.raises(ImproperlyConfigured, match='Missing bucket name'):
            GCSBackend(app=self.app)

    def test_missing_project(self):
        self.app.conf.gcs_project = None

        with pytest.raises(ImproperlyConfigured, match='Missing project'):
            GCSBackend(app=self.app)

    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_firestore_project(self, mock_firestore_ttl):
        mock_firestore_ttl.return_value = True
        b = GCSBackend(app=self.app)
        assert b.firestore_project == 'project'
        self.app.conf.firestore_project = 'project2'
        b = GCSBackend(app=self.app)
        assert b.firestore_project == 'project2'

    def test_invalid_ttl(self):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'
        self.app.conf.gcs_ttl = -1

        with pytest.raises(ImproperlyConfigured, match='Invalid ttl'):
            GCSBackend(app=self.app)

    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_firestore_ttl_policy_disabled(self, mock_firestore_ttl):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'
        self.app.conf.gcs_ttl = 0

        mock_firestore_ttl.return_value = False
        with pytest.raises(ImproperlyConfigured, match='Missing TTL policy'):
            GCSBackend(app=self.app)

    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_parse_url(self, mock_firestore_ttl, base_path):
        self.app.conf.gcs_bucket = None
        self.app.conf.gcs_project = None

        mock_firestore_ttl.return_value = True
        backend = GCSBackend(
            app=self.app,
            url=f'gcs://bucket/{base_path}?gcs_project=project',
        )
        assert backend.bucket_name == 'bucket'
        assert backend.base_path == base_path.strip('/')

    @patch.object(GCSBackend, '_is_bucket_lifecycle_rule_exists')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_bucket_ttl_missing_lifecycle_rule(
        self, mock_firestore_ttl, mock_lifecycle
    ):
        self.app.conf.gcs_ttl = 86400

        mock_lifecycle.return_value = False
        mock_firestore_ttl.return_value = True
        with pytest.raises(
            ImproperlyConfigured, match='Missing lifecycle rule'
        ):
            GCSBackend(app=self.app)
            mock_lifecycle.assert_called_once()

    @patch.object(GCSBackend, '_get_blob')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_get_key(self, mock_ttl, mock_get_blob, base_path):
        self.app.conf.gcs_base_path = base_path

        mock_ttl.return_value = True
        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        backend = GCSBackend(app=self.app)
        backend.get(b"testkey1")

        mock_get_blob.assert_called_once_with('testkey1')
        mock_blob.download_as_bytes.assert_called_once()

    @patch.object(GCSBackend, 'bucket')
    @patch.object(GCSBackend, '_get_blob')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_set_key(
        self,
        mock_firestore_ttl,
        mock_get_blob,
        mock_bucket_prop,
        base_path,
        gcs_ttl,
    ):
        self.app.conf.gcs_base_path = base_path
        self.app.conf.gcs_ttl = gcs_ttl

        mock_firestore_ttl.return_value = True
        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        mock_bucket_prop.lifecycle_rules = [{'action': {'type': 'Delete'}}]
        backend = GCSBackend(app=self.app)
        backend.set('testkey', 'testvalue')
        mock_get_blob.assert_called_once_with('testkey')
        mock_blob.upload_from_string.assert_called_once_with(
            'testvalue', retry=backend._retry_policy
        )
        if gcs_ttl:
            assert mock_blob.custom_time >= datetime.utcnow()

    @patch.object(GCSBackend, '_get_blob')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_get_missing_key(self, mock_firestore_ttl, mock_get_blob):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'

        mock_firestore_ttl.return_value = True
        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob

        mock_blob.download_as_bytes.side_effect = NotFound('not found')
        gcs_backend = GCSBackend(app=self.app)
        result = gcs_backend.get('some-key')

        assert result is None

    @patch.object(GCSBackend, '_get_blob')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_delete_existing_key(
        self, mock_firestore_ttl, mock_get_blob, base_path
    ):
        self.app.conf.gcs_base_path = base_path

        mock_firestore_ttl.return_value = True
        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        mock_blob.exists.return_value = True
        backend = GCSBackend(app=self.app)
        backend.delete(b"testkey2")

        mock_get_blob.assert_called_once_with('testkey2')
        mock_blob.exists.assert_called_once()
        mock_blob.delete.assert_called_once()

    @patch.object(GCSBackend, '_get_blob')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_delete_missing_key(
        self, mock_firestore_ttl, mock_get_blob, base_path
    ):
        self.app.conf.gcs_base_path = base_path

        mock_firestore_ttl.return_value = True
        mock_blob = Mock()
        mock_get_blob.return_value = mock_blob
        mock_blob.exists.return_value = False
        backend = GCSBackend(app=self.app)
        backend.delete(b"testkey2")

        mock_get_blob.assert_called_once_with('testkey2')
        mock_blob.exists.assert_called_once()
        mock_blob.delete.assert_not_called()

    @patch.object(GCSBackend, 'get')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_mget(self, mock_firestore_ttl, mock_get, base_path):
        self.app.conf.gcs_base_path = base_path
        mock_firestore_ttl.return_value = True
        backend = GCSBackend(app=self.app)
        mock_get.side_effect = ['value1', 'value2']
        result = backend.mget([b'key1', b'key2'])
        mock_get.assert_has_calls(
            [call(b'key1'), call(b'key2')], any_order=True
        )
        assert sorted(result) == sorted(['value1', 'value2'])

    @patch.object(GCSBackend, 'client')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_bucket(self, mock_firestore_ttl, mock_client):
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket
        mock_firestore_ttl.return_value = True
        backend = GCSBackend(app=self.app)
        result = backend.bucket
        mock_client.bucket.assert_called_once_with(backend.bucket_name)
        assert result == mock_bucket

    @patch.object(GCSBackend, 'bucket')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_get_blob(self, mock_firestore_ttl, mock_bucket):
        key = 'test_key'
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_firestore_ttl.return_value = True

        backend = GCSBackend(app=self.app)
        result = backend._get_blob(key)

        key_bucket_path = (
            f'{backend.base_path}/{key}' if backend.base_path else key
        )
        mock_bucket.blob.assert_called_once_with(key_bucket_path)
        assert result == mock_blob

    @patch('celery.backends.gcs.Client')
    @patch('celery.backends.gcs.getpid')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_new_client_after_fork(
        self, mock_firestore_ttl, mock_pid, mock_client
    ):
        mock_firestore_ttl.return_value = True
        mock_pid.return_value = 123
        backend = GCSBackend(app=self.app)
        client1 = backend.client
        assert client1 == backend.client
        mock_pid.assert_called()
        mock_client.assert_called()
        mock_pid.return_value = 456
        mock_client.return_value = Mock()
        assert client1 != backend.client
        mock_client.assert_called_with(project='project')

    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    @patch('celery.backends.gcs.firestore.Client')
    @patch('celery.backends.gcs.getpid')
    def test_new_firestore_client_after_fork(
        self, mock_pid, mock_firestore_client, mock_firestore_ttl
    ):
        mock_firestore_instance = MagicMock()
        mock_firestore_client.return_value = mock_firestore_instance

        backend = GCSBackend(app=self.app)
        mock_pid.return_value = 123
        client1 = backend.firestore_client
        client2 = backend.firestore_client

        mock_firestore_client.assert_called_once_with(
            project=backend.firestore_project
        )
        assert client1 == mock_firestore_instance
        assert client2 == mock_firestore_instance
        assert backend._pid == 123
        mock_pid.return_value = 456
        _ = backend.firestore_client
        assert backend._pid == 456

    @patch('celery.backends.gcs.firestore_admin_v1.FirestoreAdminClient')
    @patch('celery.backends.gcs.firestore_admin_v1.GetFieldRequest')
    def test_is_firestore_ttl_policy_enabled(
        self, mock_get_field_request, mock_firestore_admin_client
    ):
        mock_client_instance = MagicMock()
        mock_firestore_admin_client.return_value = mock_client_instance
        mock_field = MagicMock()
        mock_field.ttl_config.state = 2  # State.ENABLED
        mock_client_instance.get_field.return_value = mock_field

        backend = GCSBackend(app=self.app)
        result = backend._is_firestore_ttl_policy_enabled()

        assert result
        mock_field.ttl_config.state = 3  # State.NEEDS_REPAIR
        mock_client_instance.get_field.return_value = mock_field
        result = backend._is_firestore_ttl_policy_enabled()
        assert not result

    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    @patch.object(GCSBackend, '_expire_chord_key')
    @patch.object(GCSBackend, 'get_key_for_chord')
    @patch('celery.backends.gcs.KeyValueStoreBackend._apply_chord_incr')
    def test_apply_chord_incr(
        self,
        mock_super_apply_chord_incr,
        mock_get_key_for_chord,
        mock_expire_chord_key,
        mock_firestore_ttl,
    ):
        mock_firestore_ttl.return_value = True
        mock_get_key_for_chord.return_value = b'group_key'
        header_result_args = [MagicMock()]
        body = MagicMock()

        backend = GCSBackend(app=self.app)
        backend._apply_chord_incr(header_result_args, body)

        mock_get_key_for_chord.assert_called_once_with(header_result_args[0])
        mock_expire_chord_key.assert_called_once_with('group_key', 86400)
        mock_super_apply_chord_incr.assert_called_once_with(
            header_result_args, body
        )

    @patch.object(GCSBackend, '_firestore_document')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_incr(self, mock_firestore_ttl, mock_firestore_document):
        self.app.conf.gcs_bucket = 'bucket'
        self.app.conf.gcs_project = 'project'

        mock_firestore_ttl.return_value = True
        gcs_backend = GCSBackend(app=self.app)
        gcs_backend.incr(b'some-key')
        assert mock_firestore_document.call_count == 1

    @patch('celery.backends.gcs.maybe_signature')
    @patch.object(GCSBackend, 'incr')
    @patch.object(GCSBackend, '_restore_deps')
    @patch.object(GCSBackend, '_delete_chord_key')
    @patch('celery.backends.gcs.allow_join_result')
    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    def test_on_chord_part_return(
        self,
        mock_firestore_ttl,
        mock_allow_join_result,
        mock_delete_chord_key,
        mock_restore_deps,
        mock_incr,
        mock_maybe_signature,
    ):
        request = MagicMock()
        request.group = 'group_id'
        request.chord = {'chord_size': 2}
        state = MagicMock()
        result = MagicMock()
        mock_firestore_ttl.return_value = True
        mock_incr.return_value = 2
        mock_restore_deps.return_value = MagicMock()
        mock_restore_deps.return_value.join_native.return_value = [
            'result1',
            'result2',
        ]
        mock_maybe_signature.return_value = MagicMock()

        b = GCSBackend(app=self.app)
        b.on_chord_part_return(request, state, result)

        group_key = b.chord_keyprefix + b'group_id'
        mock_incr.assert_called_once_with(group_key)
        mock_restore_deps.assert_called_once_with('group_id', request)
        mock_maybe_signature.assert_called_once_with(
            request.chord, app=self.app
        )
        mock_restore_deps.return_value.join_native.assert_called_once_with(
            timeout=self.app.conf.result_chord_join_timeout,
            propagate=True,
        )
        mock_maybe_signature.return_value.delay.assert_called_once_with(
            ['result1', 'result2']
        )
        mock_delete_chord_key.assert_called_once_with(group_key)

    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    @patch('celery.backends.gcs.GroupResult.restore')
    @patch('celery.backends.gcs.maybe_signature')
    @patch.object(GCSBackend, 'chord_error_from_stack')
    def test_restore_deps(
        self,
        mock_chord_error_from_stack,
        mock_maybe_signature,
        mock_group_result_restore,
        mock_firestore_ttl,
    ):
        gid = 'group_id'
        request = MagicMock()
        mock_group_result_restore.return_value = MagicMock()

        backend = GCSBackend(app=self.app)
        deps = backend._restore_deps(gid, request)

        mock_group_result_restore.assert_called_once_with(
            gid, backend=backend
        )
        assert deps is not None
        mock_chord_error_from_stack.assert_not_called()

        mock_group_result_restore.side_effect = Exception('restore error')
        deps = backend._restore_deps(gid, request)
        mock_maybe_signature.assert_called_with(request.chord, app=self.app)
        mock_chord_error_from_stack.assert_called_once()
        assert deps is None

        mock_group_result_restore.side_effect = None
        mock_group_result_restore.return_value = None
        deps = backend._restore_deps(gid, request)
        mock_chord_error_from_stack.assert_called()
        assert deps is None

    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    @patch.object(GCSBackend, '_firestore_document')
    def test_delete_chord_key(
        self, mock_firestore_document, mock_firestore_ttl
    ):
        key = 'test_key'
        mock_document = MagicMock()
        mock_firestore_document.return_value = mock_document

        backend = GCSBackend(app=self.app)
        backend._delete_chord_key(key)

        mock_firestore_document.assert_called_once_with(key)
        mock_document.delete.assert_called_once()

    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    @patch.object(GCSBackend, '_firestore_document')
    def test_expire_chord_key(
        self, mock_firestore_document, mock_firestore_ttl
    ):
        key = 'test_key'
        expires = 86400
        mock_document = MagicMock()
        mock_firestore_document.return_value = mock_document
        expected_expiry = datetime.utcnow() + timedelta(seconds=expires)

        backend = GCSBackend(app=self.app)
        backend._expire_chord_key(key, expires)

        mock_firestore_document.assert_called_once_with(key)
        mock_document.set.assert_called_once()
        args, kwargs = mock_document.set.call_args
        assert backend._field_expires in args[0]
        assert args[0][backend._field_expires] >= expected_expiry

    @patch.object(GCSBackend, '_is_firestore_ttl_policy_enabled')
    @patch.object(GCSBackend, 'firestore_client')
    def test_firestore_document(
        self, mock_firestore_client, mock_firestore_ttl
    ):
        key = b'test_key'
        mock_collection = MagicMock()
        mock_document = MagicMock()
        mock_firestore_client.collection.return_value = mock_collection
        mock_collection.document.return_value = mock_document

        backend = GCSBackend(app=self.app)
        result = backend._firestore_document(key)

        mock_firestore_client.collection.assert_called_once_with(
            backend._collection_name
        )
        mock_collection.document.assert_called_once_with('test_key')
        assert result == mock_document
