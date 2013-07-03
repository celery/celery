from __future__ import absolute_import

from celery import messaging
from celery.tests.case import Case


class test_compat_messaging_module(Case):

    def test_get_consume_set(self):
        conn = messaging.establish_connection()
        messaging.get_consumer_set(conn).close()
        conn.close()
