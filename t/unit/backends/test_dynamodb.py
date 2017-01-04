# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
from decimal import Decimal

import pytest
from case import MagicMock, Mock, patch, sentinel, skip

from celery.backends import dynamodb as module
from celery.backends.dynamodb import DynamoDBBackend
from celery.exceptions import ImproperlyConfigured


@skip.unless_module('boto3')
class test_DynamoDBBackend:
    def setup(self):
        self._static_timestamp = Decimal(1483425566.52)  # noqa
        self.app.conf.result_backend = 'dynamodb://'

    @property
    def backend(self):
        """:rtype: DynamoDBBackend"""
        return self.app.backend

    def test_init_no_boto3(self):
        prev, module.boto3 = module.boto3, None
        try:
            with pytest.raises(ImproperlyConfigured):
                DynamoDBBackend(app=self.app)
        finally:
            module.boto3 = prev

    def test_init_aws_credentials(self):
        with pytest.raises(ImproperlyConfigured):
            DynamoDBBackend(
                app=self.app,
                url='dynamodb://a:@'
            )

    def test_get_or_create_table_not_exists(self):
        self.backend._client = MagicMock()
        mock_create_table = self.backend._client.create_table = MagicMock()
        mock_describe_table = self.backend._client.describe_table = \
            MagicMock()

        mock_describe_table.return_value = {
            'Table': {
                'TableStatus': 'ACTIVE'
            }
        }

        self.backend._get_or_create_table()
        mock_create_table.assert_called_once_with(
            **self.backend._get_table_schema()
        )

    def test_get_or_create_table_already_exists(self):
        from botocore.exceptions import ClientError

        self.backend._client = MagicMock()
        mock_create_table = self.backend._client.create_table = MagicMock()
        client_error = ClientError(
            {
                'Error': {
                    'Code': 'ResourceInUseException',
                    'Message': 'Table already exists: {}'.format(
                        self.backend.table_name
                    )
                }
            },
            'CreateTable'
        )
        mock_create_table.side_effect = client_error
        mock_describe_table = self.backend._client.describe_table = \
            MagicMock()

        mock_describe_table.return_value = {
            'Table': {
                'TableStatus': 'ACTIVE'
            }
        }

        self.backend._get_or_create_table()
        mock_describe_table.assert_called_once_with(
            TableName=self.backend.table_name
        )

    def test_wait_for_table_status(self):
        self.backend._client = MagicMock()
        mock_describe_table = self.backend._client.describe_table = \
            MagicMock()
        mock_describe_table.side_effect = [
            {'Table': {
                'TableStatus': 'CREATING'
            }},
            {'Table': {
                'TableStatus': 'SOME_STATE'
            }}
        ]
        self.backend._wait_for_table_status(expected='SOME_STATE')
        assert mock_describe_table.call_count == 2

    def test_prepare_get_request(self):
        expected = {
            'TableName': u'celery',
            'Key': {u'id': {u'S': u'abcdef'}}
        }
        assert self.backend._prepare_get_request('abcdef') == expected

    def test_prepare_put_request(self):
        expected = {
            'TableName': u'celery',
            'Item': {
                u'id': {u'S': u'abcdef'},
                u'result': {u'B': u'val'},
                u'timestamp': {
                    u'N': str(Decimal(self._static_timestamp))
                }
            }
        }
        with patch('celery.backends.dynamodb.time', self._mock_time):
            result = self.backend._prepare_put_request('abcdef', 'val')
        assert result == expected

    def test_item_to_dict(self):
        boto_response = {
            'Item': {
                'id': {
                    'S': sentinel.key
                },
                'result': {
                    'B': sentinel.value
                },
                'timestamp': {
                    'N': Decimal(1)
                }
            }
        }
        converted = self.backend._item_to_dict(boto_response)
        assert converted == {
            'id': sentinel.key,
            'result': sentinel.value,
            'timestamp': Decimal(1)
        }

    def test_get(self):
        self.backend._client = Mock(name='_client')
        self.backend._client.get_item = MagicMock()

        assert self.backend.get('1f3fab') is None
        self.backend.client.get_item.assert_called_once_with(
            Key={u'id': {u'S': u'1f3fab'}},
            TableName='celery'
        )

    def _mock_time(self):
        return self._static_timestamp

    def test_set(self):

        self.backend._client = MagicMock()
        self.backend._client.put_item = MagicMock()

        # should return None
        with patch('celery.backends.dynamodb.time', self._mock_time):
            assert self.backend.set(sentinel.key, sentinel.value) is None

        assert self.backend._client.put_item.call_count == 1
        _, call_kwargs = self.backend._client.put_item.call_args
        expected_kwargs = dict(
            Item={
                u'timestamp': {u'N': str(self._static_timestamp)},
                u'id': {u'S': sentinel.key},
                u'result': {u'B': sentinel.value}
            },
            TableName='celery'
        )
        assert call_kwargs['Item'] == expected_kwargs['Item']
        assert call_kwargs['TableName'] == 'celery'

    def test_delete(self):
        self.backend._client = Mock(name='_client')
        mocked_delete = self.backend._client.delete = Mock('client.delete')
        mocked_delete.return_value = None
        # should return None
        assert self.backend.delete('1f3fab') is None
        self.backend.client.delete_item.assert_called_once_with(
            Key={u'id': {u'S': u'1f3fab'}},
            TableName='celery'
        )

    def test_backend_by_url(self, url='dynamodb://'):
        from celery.app import backends
        from celery.backends.dynamodb import DynamoDBBackend
        backend, url_ = backends.by_url(url, self.app.loader)
        assert backend is DynamoDBBackend
        assert url_ == url

    def test_backend_params_by_url(self):
        self.app.conf.result_backend = \
            'dynamodb://@us-east-1/celery_results?read=10&write=20'
        assert self.backend.aws_region == 'us-east-1'
        assert self.backend.table_name == 'celery_results'
        assert self.backend.read_capacity_units == 10
        assert self.backend.write_capacity_units == 20
