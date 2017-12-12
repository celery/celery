from __future__ import absolute_import, unicode_literals

import datetime
from pickle import dumps, loads

import pytest
from case import ANY, MagicMock, Mock, mock, patch, sentinel, skip
from kombu.exceptions import EncodeError

from celery import states, uuid
from celery.backends.mongodb import InvalidDocument, MongoBackend
from celery.exceptions import ImproperlyConfigured

COLLECTION = 'taskmeta_celery'
TASK_ID = uuid()
MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
MONGODB_USER = 'mongo'
MONGODB_PASSWORD = '1234'
MONGODB_DATABASE = 'testing'
MONGODB_COLLECTION = 'collection1'
MONGODB_GROUP_COLLECTION = 'group_collection1'


@skip.unless_module('pymongo')
class test_MongoBackend:

    default_url = 'mongodb://uuuu:pwpw@hostname.dom/database'
    replica_set_url = (
        'mongodb://uuuu:pwpw@hostname.dom,'
        'hostname.dom/database?replicaSet=rs'
    )
    sanitized_default_url = 'mongodb://uuuu:**@hostname.dom/database'
    sanitized_replica_set_url = (
        'mongodb://uuuu:**@hostname.dom/,'
        'hostname.dom/database?replicaSet=rs'
    )

    def setup(self):
        self.patching('celery.backends.mongodb.MongoBackend.encode')
        self.patching('celery.backends.mongodb.MongoBackend.decode')
        self.patching('celery.backends.mongodb.Binary')
        self.patching('datetime.datetime')
        self.backend = MongoBackend(app=self.app, url=self.default_url)

    def test_init_no_mongodb(self, patching):
        patching('celery.backends.mongodb.pymongo', None)
        with pytest.raises(ImproperlyConfigured):
            MongoBackend(app=self.app)

    def test_init_no_settings(self):
        self.app.conf.mongodb_backend_settings = []
        with pytest.raises(ImproperlyConfigured):
            MongoBackend(app=self.app)

    def test_init_settings_is_None(self):
        self.app.conf.mongodb_backend_settings = None
        MongoBackend(app=self.app)

    def test_init_with_settings(self):
        self.app.conf.mongodb_backend_settings = None
        # empty settings
        mb = MongoBackend(app=self.app)

        # uri
        uri = 'mongodb://localhost:27017'
        mb = MongoBackend(app=self.app, url=uri)
        assert mb.mongo_host == ['localhost:27017']
        assert mb.options == mb._prepare_client_options()
        assert mb.database_name == 'celery'

        # uri with database name
        uri = 'mongodb://localhost:27017/celerydb'
        mb = MongoBackend(app=self.app, url=uri)
        assert mb.database_name == 'celerydb'

        # uri with user, password, database name, replica set
        uri = ('mongodb://'
               'celeryuser:celerypassword@'
               'mongo1.example.com:27017,'
               'mongo2.example.com:27017,'
               'mongo3.example.com:27017/'
               'celerydatabase?replicaSet=rs0')
        mb = MongoBackend(app=self.app, url=uri)
        assert mb.mongo_host == [
            'mongo1.example.com:27017',
            'mongo2.example.com:27017',
            'mongo3.example.com:27017',
        ]
        assert mb.options == dict(
            mb._prepare_client_options(),
            replicaset='rs0',
        )
        assert mb.user == 'celeryuser'
        assert mb.password == 'celerypassword'
        assert mb.database_name == 'celerydatabase'

        # same uri, change some parameters in backend settings
        self.app.conf.mongodb_backend_settings = {
            'replicaset': 'rs1',
            'user': 'backenduser',
            'database': 'another_db',
            'options': {
                'socketKeepAlive': True,
            },
        }
        mb = MongoBackend(app=self.app, url=uri)
        assert mb.mongo_host == [
            'mongo1.example.com:27017',
            'mongo2.example.com:27017',
            'mongo3.example.com:27017',
        ]
        assert mb.options == dict(
            mb._prepare_client_options(),
            replicaset='rs1',
            socketKeepAlive=True,
        )
        assert mb.user == 'backenduser'
        assert mb.password == 'celerypassword'
        assert mb.database_name == 'another_db'

        mb = MongoBackend(app=self.app, url='mongodb://')

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce(self):
        x = MongoBackend(app=self.app)
        assert loads(dumps(x))

    def test_get_connection_connection_exists(self):
        with patch('pymongo.MongoClient') as mock_Connection:
            self.backend._connection = sentinel._connection

            connection = self.backend._get_connection()

            assert sentinel._connection == connection
            mock_Connection.assert_not_called()

    def test_get_connection_no_connection_host(self):
        with patch('pymongo.MongoClient') as mock_Connection:
            self.backend._connection = None
            self.backend.host = MONGODB_HOST
            self.backend.port = MONGODB_PORT
            mock_Connection.return_value = sentinel.connection

            connection = self.backend._get_connection()
            mock_Connection.assert_called_once_with(
                host='mongodb://localhost:27017',
                **self.backend._prepare_client_options()
            )
            assert sentinel.connection == connection

    def test_get_connection_no_connection_mongodb_uri(self):
        with patch('pymongo.MongoClient') as mock_Connection:
            mongodb_uri = 'mongodb://%s:%d' % (MONGODB_HOST, MONGODB_PORT)
            self.backend._connection = None
            self.backend.host = mongodb_uri

            mock_Connection.return_value = sentinel.connection

            connection = self.backend._get_connection()
            mock_Connection.assert_called_once_with(
                host=mongodb_uri, **self.backend._prepare_client_options()
            )
            assert sentinel.connection == connection

    @patch('celery.backends.mongodb.MongoBackend._get_connection')
    def test_get_database_no_existing(self, mock_get_connection):
        # Should really check for combinations of these two, to be complete.
        self.backend.user = MONGODB_USER
        self.backend.password = MONGODB_PASSWORD

        mock_database = Mock()
        mock_connection = MagicMock(spec=['__getitem__'])
        mock_connection.__getitem__.return_value = mock_database
        mock_get_connection.return_value = mock_connection

        database = self.backend.database

        assert database is mock_database
        assert self.backend.__dict__['database'] is mock_database
        mock_database.authenticate.assert_called_once_with(
            MONGODB_USER, MONGODB_PASSWORD)

    @patch('celery.backends.mongodb.MongoBackend._get_connection')
    def test_get_database_no_existing_no_auth(self, mock_get_connection):
        # Should really check for combinations of these two, to be complete.
        self.backend.user = None
        self.backend.password = None

        mock_database = Mock()
        mock_connection = MagicMock(spec=['__getitem__'])
        mock_connection.__getitem__.return_value = mock_database
        mock_get_connection.return_value = mock_connection

        database = self.backend.database

        assert database is mock_database
        mock_database.authenticate.assert_not_called()
        assert self.backend.__dict__['database'] is mock_database

    @patch('celery.backends.mongodb.MongoBackend._get_database')
    def test_store_result(self, mock_get_database):
        self.backend.taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._store_result(
            sentinel.task_id, sentinel.result, sentinel.status)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(MONGODB_COLLECTION)
        mock_collection.save.assert_called_once_with(ANY)
        assert sentinel.result == ret_val

        mock_collection.save.side_effect = InvalidDocument()
        with pytest.raises(EncodeError):
            self.backend._store_result(
                sentinel.task_id, sentinel.result, sentinel.status)

    @patch('celery.backends.mongodb.MongoBackend._get_database')
    def test_get_task_meta_for(self, mock_get_database):
        self.backend.taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()
        mock_collection.find_one.return_value = MagicMock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._get_task_meta_for(sentinel.task_id)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(MONGODB_COLLECTION)
        assert list(sorted([
            'status', 'task_id', 'date_done',
            'traceback', 'result', 'children',
        ])) == list(sorted(ret_val.keys()))

    @patch('celery.backends.mongodb.MongoBackend._get_database')
    def test_get_task_meta_for_no_result(self, mock_get_database):
        self.backend.taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()
        mock_collection.find_one.return_value = None

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._get_task_meta_for(sentinel.task_id)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(MONGODB_COLLECTION)
        assert {'status': states.PENDING, 'result': None} == ret_val

    @patch('celery.backends.mongodb.MongoBackend._get_database')
    def test_save_group(self, mock_get_database):
        self.backend.groupmeta_collection = MONGODB_GROUP_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        res = [self.app.AsyncResult(i) for i in range(3)]
        ret_val = self.backend._save_group(
            sentinel.taskset_id, res,
        )
        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(
            MONGODB_GROUP_COLLECTION,
        )
        mock_collection.save.assert_called_once_with(ANY)
        assert res == ret_val

    @patch('celery.backends.mongodb.MongoBackend._get_database')
    def test_restore_group(self, mock_get_database):
        self.backend.groupmeta_collection = MONGODB_GROUP_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()
        mock_collection.find_one.return_value = {
            '_id': sentinel.taskset_id,
            'result': [uuid(), uuid()],
            'date_done': 1,
        }
        self.backend.decode.side_effect = lambda r: r

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        ret_val = self.backend._restore_group(sentinel.taskset_id)

        mock_get_database.assert_called_once_with()
        mock_collection.find_one.assert_called_once_with(
            {'_id': sentinel.taskset_id})
        assert (sorted(['date_done', 'result', 'task_id']) ==
                sorted(list(ret_val.keys())))

        mock_collection.find_one.return_value = None
        self.backend._restore_group(sentinel.taskset_id)

    @patch('celery.backends.mongodb.MongoBackend._get_database')
    def test_delete_group(self, mock_get_database):
        self.backend.taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        self.backend._delete_group(sentinel.taskset_id)

        mock_get_database.assert_called_once_with()
        mock_collection.remove.assert_called_once_with(
            {'_id': sentinel.taskset_id})

    @patch('celery.backends.mongodb.MongoBackend._get_database')
    def test_forget(self, mock_get_database):
        self.backend.taskmeta_collection = MONGODB_COLLECTION

        mock_database = MagicMock(spec=['__getitem__', '__setitem__'])
        mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection

        self.backend._forget(sentinel.task_id)

        mock_get_database.assert_called_once_with()
        mock_database.__getitem__.assert_called_once_with(
            MONGODB_COLLECTION)
        mock_collection.remove.assert_called_once_with(
            {'_id': sentinel.task_id})

    @patch('celery.backends.mongodb.MongoBackend._get_database')
    def test_cleanup(self, mock_get_database):
        self.backend.taskmeta_collection = MONGODB_COLLECTION
        self.backend.groupmeta_collection = MONGODB_GROUP_COLLECTION

        mock_database = Mock(spec=['__getitem__', '__setitem__'],
                             name='MD')
        self.backend.collections = mock_collection = Mock()

        mock_get_database.return_value = mock_database
        mock_database.__getitem__ = Mock(name='MD.__getitem__')
        mock_database.__getitem__.return_value = mock_collection

        self.backend.app.now = datetime.datetime.utcnow
        self.backend.cleanup()

        mock_get_database.assert_called_once_with()
        mock_collection.remove.assert_called()

    def test_get_database_authfailure(self):
        x = MongoBackend(app=self.app)
        x._get_connection = Mock()
        conn = x._get_connection.return_value = {}
        db = conn[x.database_name] = Mock()
        db.authenticate.return_value = False
        x.user = 'jerry'
        x.password = 'cere4l'
        with pytest.raises(ImproperlyConfigured):
            x._get_database()
        db.authenticate.assert_called_with('jerry', 'cere4l')

    def test_prepare_client_options(self):
        with patch('pymongo.version_tuple', new=(3, 0, 3)):
            options = self.backend._prepare_client_options()
            assert options == {
                'maxPoolSize': self.backend.max_pool_size
            }

    def test_as_uri_include_password(self):
        assert self.backend.as_uri(True) == self.default_url

    def test_as_uri_exclude_password(self):
        assert self.backend.as_uri() == self.sanitized_default_url

    def test_as_uri_include_password_replica_set(self):
        backend = MongoBackend(app=self.app, url=self.replica_set_url)
        assert backend.as_uri(True) == self.replica_set_url

    def test_as_uri_exclude_password_replica_set(self):
        backend = MongoBackend(app=self.app, url=self.replica_set_url)
        assert backend.as_uri() == self.sanitized_replica_set_url

    def test_regression_worker_startup_info(self):
        self.app.conf.result_backend = (
            'mongodb://user:password@host0.com:43437,host1.com:43437'
            '/work4us?replicaSet=rs&ssl=true'
        )
        worker = self.app.Worker()
        with mock.stdouts():
            worker.on_start()
            assert worker.startup_info()


@skip.unless_module('pymongo')
class test_MongoBackend_no_mock:

    def test_encode_decode(self, app):
        backend = MongoBackend(app=app)
        data = {'foo': 1}
        assert backend.decode(backend.encode(data))
        backend.serializer = 'bson'
        assert backend.encode(data) == data
        assert backend.decode(data) == data

    def test_de(self, app):
        backend = MongoBackend(app=app)
        data = {'foo': 1}
        assert backend.encode(data)
        backend.serializer = 'bson'
        assert backend.encode(data) == data
