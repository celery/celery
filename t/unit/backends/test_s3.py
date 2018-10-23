from __future__ import absolute_import, unicode_literals

from case import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

from celery.backends.s3 import S3Backend
from celery.exceptions import ImproperlyConfigured


class test_S3Backend:
    def test_with_missing_aws_credentials(self):
        self.app.conf.s3_access_key_id = None
        self.app.conf.s3_secret_access_key = None

        with pytest.raises(ImproperlyConfigured, match="Missing aws s3 creds"):
            S3Backend(app=self.app)

    def test_with_a_missing_bucket(self):
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

    @patch('celery.backends.s3.boto3')
    def test_get_a_missing_key(self, mock_boto3):
        error = ClientError({'Error': {'Code': '404',
                                       'Message': 'Object not found'}},
                            'error')
        mock_boto3.Session().resource().Object().load.side_effect = error

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        result = s3_backend.get('uuidddd')

        assert result is None

    @patch('celery.backends.s3.boto3')
    def test_get_a_key(self, mock_boto3):
        stream_body = MagicMock()
        stream_body.read.return_value = b'a_status'
        s3_object = {'Body': stream_body}
        mock_boto3.Session().resource().Object().get.return_value = s3_object

        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        result = s3_backend.get('uuidddd')

        assert result == 'a_status'

    @patch('celery.backends.s3.boto3')
    def test_set(self, mock_boto3):
        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        s3_backend.set('uuid', 'a_status')

        s3_object = mock_boto3.Session().resource().Object()
        s3_object.put.assert_called_once_with(Body='a_status')

    @patch('celery.backends.s3.boto3')
    def test_delete(self, mock_boto3):
        self.app.conf.s3_access_key_id = 'somekeyid'
        self.app.conf.s3_secret_access_key = 'somesecret'
        self.app.conf.s3_bucket = 'bucket'

        s3_backend = S3Backend(app=self.app)
        s3_backend.delete('uuid')

        s3_object = mock_boto3.Session().resource().Object()
        s3_object.delete.assert_called_once()
