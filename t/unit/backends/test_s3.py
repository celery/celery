from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_s3

from celery import states
from celery.backends.s3 import S3Backend
from celery.exceptions import ImproperlyConfigured


class test_S3Backend:

    @patch('botocore.credentials.CredentialResolver.load_credentials')
    def test_with_missing_aws_credentials(self, mock_load_credentials):
        self.app.conf.s3_access_key_id = None
        self.app.conf.s3_secret_access_key = None
        self.app.conf.s3_bucket = 'bucket'

        mock_load_credentials.return_value = None

        with pytest.raises(ImproperlyConfigured, match="Missing aws s3 creds"):
            S3Backend(app=self.app)

    @patch('botocore.credentials.CredentialResolver.load_credentials')
    def test_with_no_credentials_in_config_attempts_to_load_credentials(self, mock_load_credentials):
        self.app.conf.s3_access_key_id = None
        self.app.conf.s3_secret_access_key = None
        self.app.conf.s3_bucket = 'bucket'

        S3Backend(app=self.app)
        mock_load_credentials.assert_called_once()

    @patch('botocore.credentials.CredentialResolver.load_credentials')
    def test_with_credentials_in_config_does_not_search_for_credentials(self, mock_load_credentials):
        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        S3Backend(app=self.app)
        mock_load_credentials.assert_not_called()

    def test_with_no_given_bucket(self):
        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = None

        with pytest.raises(ImproperlyConfigured, match='Missing bucket name'):
            S3Backend(app=self.app)

    @pytest.mark.parametrize('aws_region',
                             [None, 'us-east-1'],
                             ids=['No given aws region',
                                  'Specific aws region'])
    @patch('celery.backends.s3.boto3')
    def test_it_creates_an_aws_s3_connection(self, mock_boto3, aws_region):
        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'
        self.app.conf.s3_region = aws_region

        S3Backend(app=self.app)
        mock_boto3.Session.assert_called_once_with(
            aws_access_key_id='somekeyid',
            aws_secret_access_key='somesecret',
            region_name=aws_region)

    @pytest.mark.parametrize('endpoint_url',
                             [None, 'https://custom.s3'],
                             ids=['No given endpoint url',
                                  'Custom endpoint url'])
    @patch('celery.backends.s3.boto3')
    def test_it_creates_an_aws_s3_resource(self,
                                           mock_boto3,
                                           endpoint_url):
        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'
        self.app.conf.s3_endpoint_url = endpoint_url

        S3Backend(app=self.app)
        mock_boto3.Session().resource.assert_called_once_with(
            's3', endpoint_url=endpoint_url)

    @pytest.mark.parametrize("key", ['uuid', b'uuid'])
    @mock_s3
    def test_set_and_get_a_key(self, key):
        self._mock_s3_resource()

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        s3_backend._set_with_state(key, 'another_status', states.SUCCESS)

        assert s3_backend.get(key) == 'another_status'

    @mock_s3
    def test_set_and_get_a_result(self):
        self._mock_s3_resource()

        self.app.conf.result_serializer = 'pickle'
        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        s3_backend.store_result('foo', 'baar', 'STARTED')
        value = s3_backend.get_result('foo')
        assert value == 'baar'

    @mock_s3
    def test_get_a_missing_key(self):
        self._mock_s3_resource()

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        result = s3_backend.get('uuidddd')

        assert result is None

    @patch('celery.backends.s3.boto3')
    def test_with_error_while_getting_key(self, mock_boto3):
        error = ClientError({'Error': {'Code': '403',
                                       'Message': 'Permission denied'}},
                            'error')
        mock_boto3.Session().resource().Object().load.side_effect = error

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)

        with pytest.raises(ClientError):
            s3_backend.get('uuidddd')

    @pytest.mark.parametrize("key", ['uuid', b'uuid'])
    @mock_s3
    def test_delete_a_key(self, key):
        self._mock_s3_resource()

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        s3_backend._set_with_state(key, 'another_status', states.SUCCESS)
        assert s3_backend.get(key) == 'another_status'

        s3_backend.delete(key)

        assert s3_backend.get(key) is None

    @mock_s3
    def test_with_a_non_existing_bucket(self):
        self._mock_s3_resource()

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket_not_exists'

        s3_backend = S3Backend(app=self.app)

        with pytest.raises(ClientError,
                           match=r'.*The specified bucket does not exist'):
            s3_backend._set_with_state('uuid', 'another_status', states.SUCCESS)

    def _mock_s3_resource(self):
        # Create AWS s3 Bucket for moto.
        session = boto3.Session(
            aws_access_key_id='moto_key_id',
            aws_secret_access_key='moto_secret_key',
            region_name='us-east-1'
        )
        s3 = session.resource('s3')
        s3.create_bucket(Bucket='bucket')
