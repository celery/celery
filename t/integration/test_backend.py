from __future__ import absolute_import, unicode_literals

import os

from case import skip
from pytest import fixture
from uuid import uuid1

from sqlalchemy import create_engine

from celery import states
from celery.backends.azureblockblob import AzureBlockBlobBackend
from celery.backends.database import DatabaseBackend


@skip.unless_module("azure")
@skip.unless_environ("AZUREBLOCKBLOB_URL")
class test_AzureBlockBlobBackend:
    def test_crud(self, manager):
        backend = AzureBlockBlobBackend(
            app=manager.app,
            url=os.environ["AZUREBLOCKBLOB_URL"])

        key_values = {("akey%d" % i).encode(): "avalue%d" % i
                      for i in range(5)}

        for key, value in key_values.items():
            backend.set(key, value)

        actual_values = backend.mget(key_values.keys())
        expected_values = list(key_values.values())

        assert expected_values == actual_values

        for key in key_values:
            backend.delete(key)

    def test_get_missing(self, manager):
        backend = AzureBlockBlobBackend(
            app=manager.app,
            url=os.environ["AZUREBLOCKBLOB_URL"])

        assert backend.get(b"doesNotExist") is None


@skip.unless_module("sqlalchemy")
@skip.unless_module("MySQLdb")
@skip.unless_environ("MYSQL_URL")
class test_DatabaseMySQLBackend:

    @fixture(scope='function')
    def extended_app(self, app):
        app.conf.result_serializer = 'pickle'
        app.conf.database_large_result_storage = True
        yield app

    @fixture(scope='class')
    def mysql_url(self):
        yield os.environ["MYSQL_URL"]
    
    def clear_db(self, url):
        engine = create_engine(url)
        engine.execute('DROP TABLE IF EXISTS celery_taskmeta;')
        engine.execute('DROP TABLE IF EXISTS celery_tasksetmeta;')
        engine.dispose()

    # def test_store_result(self, mysql_url, app):
    #     self.clear_db(mysql_url)
    #     tb = DatabaseBackend(mysql_url, app=app)
    #     tb.store_result(uuid1(), {'fizz': 'buzz'}, states.SUCCESS)

    def test_store_extended_result(self, mysql_url, extended_app):

        self.clear_db(mysql_url)
        tb = DatabaseBackend(mysql_url, app=extended_app)

        print vars(tb)
        # By default, celery stores results in MySQL in a BLOB column, with
        # max length 2**16 - 1.
        # See: https://dev.mysql.com/doc/refman/5.5/en/storage-requirements.html#data-types-storage-reqs-strings
        long_result = 'a'*2**17

        tb.store_result(uuid1, long_result, states.SUCCESS)

