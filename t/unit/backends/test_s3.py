from __future__ import absolute_import, unicode_literals

import boto3
import pytest
from botocore.exceptions import ClientError
from case import patch
from moto import mock_s3

from celery.backends.s3 import S3Backend
from celery.exceptions import ImproperlyConfigured


class test_S3Backend:

    def test_with_missing_aws_credentials(self):
        self.app.conf.s3_access_key_id = None
        self.app.conf.s3_secret_access_key = None

        with pytest.raises(ImproperlyConfigured, match="Missing aws s3 creds"):
            S3Backend(app=self.app)

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

    @mock_s3
    def test_set_and_get_a_key(self):
        self._mock_s3_resource()

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        s3_backend.set('uuid', 'another_status')

        assert s3_backend.get('uuid') == 'another_status'

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

    @mock_s3
    def test_delete_a_key(self):
        self._mock_s3_resource()

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        s3_backend.set('uuid', 'another_status')
        assert s3_backend.get('uuid') == 'another_status'

        s3_backend.delete('uuid')

        assert s3_backend.get('uuid') is None

    @mock_s3
    def test_with_a_non_existing_bucket(self):
        self._mock_s3_resource()

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket_not_exists'

        s3_backend = S3Backend(app=self.app)

        with pytest.raises(ClientError,
                           match=r'.*The specified bucket does not exist'):
            s3_backend.set('uuid', 'another_status')

    def _mock_s3_resource(self):
        # Create AWS s3 Bucket for moto.
        session = boto3.Session(
            aws_access_key_id='moto_key_id',
            aws_secret_access_key='moto_secret_key',
            region_name='us-east-1'
        )
        s3 = session.resource('s3')
        s3.create_bucket(Bucket='bucket')
