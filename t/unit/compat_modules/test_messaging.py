import pytest

from celery import messaging


@pytest.mark.usefixtures('depends_on_current_app')
class test_compat_messaging_module:

    def test_get_consume_set(self):
        conn = messaging.establish_connection()
        messaging.get_consumer_set(conn).close()
        conn.close()
