# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

from decimal import Decimal

import pytest
from case import MagicMock, Mock, patch, sentinel, skip

from celery import states
from celery.backends import dynamodb as module
from celery.backends.dynamodb import DynamoDBBackend
from celery.exceptions import ImproperlyConfigured
from celery.five import string


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

    def test_init_invalid_ttl_seconds_raises(self):
        with pytest.raises(ValueError):
            DynamoDBBackend(
                app=self.app,
                url='dynamodb://@?ttl_seconds=1d'
            )

    def test_get_client_explicit_endpoint(self):
        table_creation_path = \
            'celery.backends.dynamodb.DynamoDBBackend._get_or_create_table'
        with patch('boto3.client') as mock_boto_client, \
                patch(table_creation_path):

            self.app.conf.dynamodb_endpoint_url = 'http://my.domain.com:666'
            backend = DynamoDBBackend(
                app=self.app,
                url='dynamodb://@us-east-1'
            )
            client = backend._get_client()
            assert backend.client is client
            mock_boto_client.assert_called_once_with(
                'dynamodb',
                endpoint_url='http://my.domain.com:666',
                region_name='us-east-1'
            )
            assert backend.endpoint_url == 'http://my.domain.com:666'

    def test_get_client_local(self):
        table_creation_path = \
            'celery.backends.dynamodb.DynamoDBBackend._get_or_create_table'
        with patch('boto3.client') as mock_boto_client, \
                patch(table_creation_path):
            backend = DynamoDBBackend(
                app=self.app,
                url='dynamodb://@localhost:8000'
            )
            client = backend._get_client()
            assert backend.client is client
            mock_boto_client.assert_called_once_with(
                'dynamodb',
                endpoint_url='http://localhost:8000',
                region_name='us-east-1'
            )
            assert backend.endpoint_url == 'http://localhost:8000'

    def test_get_client_credentials(self):
        table_creation_path = \
            'celery.backends.dynamodb.DynamoDBBackend._get_or_create_table'
        with patch('boto3.client') as mock_boto_client, \
                patch(table_creation_path):
            backend = DynamoDBBackend(
                app=self.app,
                url='dynamodb://key:secret@test'
            )
            client = backend._get_client()
            assert client is backend.client
            mock_boto_client.assert_called_once_with(
                'dynamodb',
                aws_access_key_id='key',
                aws_secret_access_key='secret',
                region_name='test'
            )
            assert backend.aws_region == 'test'

    @patch('boto3.client')
    @patch('celery.backends.dynamodb.DynamoDBBackend._get_or_create_table')
    @patch('celery.backends.dynamodb.DynamoDBBackend._validate_ttl_methods')
    @patch('celery.backends.dynamodb.DynamoDBBackend._set_table_ttl')
    def test_get_client_time_to_live_called(
            self,
            mock_set_table_ttl,
            mock_validate_ttl_methods,
            mock_get_or_create_table,
            mock_boto_client,
    ):
        backend = DynamoDBBackend(
            app=self.app,
            url='dynamodb://key:secret@test?ttl_seconds=30'
        )
        backend._get_client()

        mock_validate_ttl_methods.assert_called_once()
        mock_set_table_ttl.assert_called_once()

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

    def test_has_ttl_none_returns_none(self):
        self.backend.time_to_live_seconds = None
        assert self.backend._has_ttl() is None

    def test_has_ttl_lt_zero_returns_false(self):
        self.backend.time_to_live_seconds = -1
        assert self.backend._has_ttl() is False

    def test_has_ttl_gte_zero_returns_true(self):
        self.backend.time_to_live_seconds = 30
        assert self.backend._has_ttl() is True

    def test_validate_ttl_methods_present_returns_none(self):
        self.backend._client = MagicMock()
        assert self.backend._validate_ttl_methods() is None

    def test_validate_ttl_methods_missing_raise(self):
        self.backend._client = MagicMock()
        delattr(self.backend._client, 'describe_time_to_live')
        delattr(self.backend._client, 'update_time_to_live')

        with pytest.raises(AttributeError):
            self.backend._validate_ttl_methods()

        with pytest.raises(AttributeError):
            self.backend._validate_ttl_methods()

    def test_set_table_ttl_describe_time_to_live_fails_raises(self):
        from botocore.exceptions import ClientError

        self.backend.time_to_live_seconds = -1
        self.backend._client = MagicMock()
        mock_describe_time_to_live = \
            self.backend._client.describe_time_to_live = MagicMock()
        client_error = ClientError(
            {
                'Error': {
                    'Code': 'Foo',
                    'Message': 'Bar',
                }
            },
            'DescribeTimeToLive'
        )
        mock_describe_time_to_live.side_effect = client_error

        with pytest.raises(ClientError):
            self.backend._set_table_ttl()

    def test_set_table_ttl_enable_when_disabled_succeeds(self):
        self.backend.time_to_live_seconds = 30
        self.backend._client = MagicMock()
        mock_update_time_to_live = self.backend._client.update_time_to_live = \
            MagicMock()

        mock_describe_time_to_live = \
            self.backend._client.describe_time_to_live = MagicMock()
        mock_describe_time_to_live.return_value = {
            'TimeToLiveDescription': {
                'TimeToLiveStatus': 'DISABLED',
                'AttributeName': self.backend._ttl_field.name
            }
        }

        self.backend._set_table_ttl()
        mock_describe_time_to_live.assert_called_once_with(
            TableName=self.backend.table_name
        )
        mock_update_time_to_live.assert_called_once()

    def test_set_table_ttl_enable_when_enabled_with_correct_attr_succeeds(self):
        self.backend.time_to_live_seconds = 30
        self.backend._client = MagicMock()
        self.backend._client.update_time_to_live = MagicMock()

        mock_describe_time_to_live = \
            self.backend._client.describe_time_to_live = MagicMock()
        mock_describe_time_to_live.return_value = {
            'TimeToLiveDescription': {
                'TimeToLiveStatus': 'ENABLED',
                'AttributeName': self.backend._ttl_field.name
            }
        }

        self.backend._set_table_ttl()
        mock_describe_time_to_live.assert_called_once_with(
            TableName=self.backend.table_name
        )

    def test_set_table_ttl_enable_when_currently_disabling_raises(self):
        from botocore.exceptions import ClientError

        self.backend.time_to_live_seconds = 30
        self.backend._client = MagicMock()
        mock_update_time_to_live = self.backend._client.update_time_to_live = \
            MagicMock()
        client_error = ClientError(
            {
                'Error': {
                    'Code': 'ValidationException',
                    'Message': (
                        'Time to live has been modified multiple times '
                        'within a fixed interval'
                    )
                }
            },
            'UpdateTimeToLive'
        )
        mock_update_time_to_live.side_effect = client_error

        mock_describe_time_to_live = \
            self.backend._client.describe_time_to_live = MagicMock()
        mock_describe_time_to_live.return_value = {
            'TimeToLiveDescription': {
                'TimeToLiveStatus': 'DISABLING',
                'AttributeName': self.backend._ttl_field.name
            }
        }

        with pytest.raises(ClientError):
            self.backend._set_table_ttl()

    def test_set_table_ttl_enable_when_enabled_with_wrong_attr_raises(self):
        from botocore.exceptions import ClientError

        self.backend.time_to_live_seconds = 30
        self.backend._client = MagicMock()
        mock_update_time_to_live = self.backend._client.update_time_to_live = \
            MagicMock()
        wrong_attr_name = self.backend._ttl_field.name + 'x'
        client_error = ClientError(
            {
                'Error': {
                    'Code': 'ValidationException',
                    'Message': (
                        'TimeToLive is active on a different AttributeName: '
                        'current AttributeName is {}'
                    ).format(wrong_attr_name)
                }
            },
            'UpdateTimeToLive'
        )
        mock_update_time_to_live.side_effect = client_error
        mock_describe_time_to_live = \
            self.backend._client.describe_time_to_live = MagicMock()

        mock_describe_time_to_live.return_value = {
            'TimeToLiveDescription': {
                'TimeToLiveStatus': 'ENABLED',
                'AttributeName': self.backend._ttl_field.name + 'x'
            }
        }

        with pytest.raises(ClientError):
            self.backend._set_table_ttl()

    def test_set_table_ttl_disable_when_disabled_succeeds(self):
        self.backend.time_to_live_seconds = -1
        self.backend._client = MagicMock()
        self.backend._client.update_time_to_live = MagicMock()
        mock_describe_time_to_live = \
            self.backend._client.describe_time_to_live = MagicMock()

        mock_describe_time_to_live.return_value = {
            'TimeToLiveDescription': {
                'TimeToLiveStatus': 'DISABLED'
            }
        }

        self.backend._set_table_ttl()
        mock_describe_time_to_live.assert_called_once_with(
            TableName=self.backend.table_name
        )

    def test_set_table_ttl_disable_when_currently_enabling_raises(self):
        from botocore.exceptions import ClientError

        self.backend.time_to_live_seconds = -1
        self.backend._client = MagicMock()
        mock_update_time_to_live = self.backend._client.update_time_to_live = \
            MagicMock()
        client_error = ClientError(
            {
                'Error': {
                    'Code': 'ValidationException',
                    'Message': (
                        'Time to live has been modified multiple times '
                        'within a fixed interval'
                    )
                }
            },
            'UpdateTimeToLive'
        )
        mock_update_time_to_live.side_effect = client_error

        mock_describe_time_to_live = \
            self.backend._client.describe_time_to_live = MagicMock()
        mock_describe_time_to_live.return_value = {
            'TimeToLiveDescription': {
                'TimeToLiveStatus': 'ENABLING',
                'AttributeName': self.backend._ttl_field.name
            }
        }

        with pytest.raises(ClientError):
            self.backend._set_table_ttl()

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

    def test_prepare_put_request_with_ttl(self):
        ttl = self.backend.time_to_live_seconds = 30
        expected = {
            'TableName': u'celery',
            'Item': {
                u'id': {u'S': u'abcdef'},
                u'result': {u'B': u'val'},
                u'timestamp': {
                    u'N': str(Decimal(self._static_timestamp))
                },
                u'ttl': {
                    u'N': str(int(self._static_timestamp + ttl))
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
            assert self.backend._set_with_state(sentinel.key, sentinel.value, states.SUCCESS) is None

        assert self.backend._client.put_item.call_count == 1
        _, call_kwargs = self.backend._client.put_item.call_args
        expected_kwargs = {
            'Item': {
                u'timestamp': {u'N': str(self._static_timestamp)},
                u'id': {u'S': string(sentinel.key)},
                u'result': {u'B': sentinel.value}
            },
            'TableName': 'celery'
        }
        assert call_kwargs['Item'] == expected_kwargs['Item']
        assert call_kwargs['TableName'] == 'celery'

    def test_set_with_ttl(self):
        ttl = self.backend.time_to_live_seconds = 30

        self.backend._client = MagicMock()
        self.backend._client.put_item = MagicMock()

        # should return None
        with patch('celery.backends.dynamodb.time', self._mock_time):
            assert self.backend._set_with_state(sentinel.key, sentinel.value, states.SUCCESS) is None

        assert self.backend._client.put_item.call_count == 1
        _, call_kwargs = self.backend._client.put_item.call_args
        expected_kwargs = {
            'Item': {
                u'timestamp': {u'N': str(self._static_timestamp)},
                u'id': {u'S': string(sentinel.key)},
                u'result': {u'B': sentinel.value},
                u'ttl': {u'N': str(int(self._static_timestamp + ttl))},
            },
            'TableName': 'celery'
        }
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
        self.app.conf.result_backend = (
            'dynamodb://@us-east-1/celery_results'
            '?read=10'
            '&write=20'
            '&ttl_seconds=600'
        )
        assert self.backend.aws_region == 'us-east-1'
        assert self.backend.table_name == 'celery_results'
        assert self.backend.read_capacity_units == 10
        assert self.backend.write_capacity_units == 20
        assert self.backend.time_to_live_seconds == 600
        assert self.backend.endpoint_url is None
