import settings
from django.core.management import setup_environ
setup_environ(settings)
from carrot.connection import DjangoAMQPConnection
from carrot.messaging import Messaging
from amqplib import client_0_8 as amqp
from celery.task import dmap
import operator
import simplejson
import time
import multiprocessing
import logging


def get_logger():
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)
    multiprocessing.log_to_stderr()
    return logger


class MyMessager(Messaging):
    queue = "conntest"
    exchange = "conntest"
    routing_key = "conntest"


def _create_conn():
    from django.conf import settings
    conn = amqp.Connection(host=settings.AMQP_SERVER,
                           userid=settings.AMQP_USER,
                           password=settings.AMQP_PASSWORD,
                           virtual_host=settings.AMQP_VHOST,
                           insist=False)
    return conn


def _send2(msg):
    conn = _create_conn()
    channel = conn.channel()
    msg = amqp.Message(msg)
    msg.properties["delivery_mode"] = 2
    channel.basic_publish(msg, exchange="conntest", routing_key="conntest")
    conn.close()


def _recv2():
    conn = _create_conn()
    channel = conn.channel()
    channel.queue_declare(queue="conntest", durable=True, exclusive=False,
                          auto_delete=False)
    channel.exchange_declare(exchange="conntest", type="direct",
                             durable=True, auto_delete=False)
    channel.queue_bind(queue="conntest", exchange="conntest",
                       routing_key="conntest")
    m = channel.basic_get("conntest")
    if m:
        channel.basic_ack(m.delivery_tag)
        print("RECEIVED MSG: %s" % m.body)
    conn.close()


def send_a_message(msg):
    conn = DjangoAMQPConnection()
    MyMessager(connection=conn).send({"message": msg})
    conn.close()


def discard_all():
    conn = DjangoAMQPConnection()
    MyMessager(connection=conn).consumer.discard_all()
    conn.close()


def receive_a_message():
    logger = get_logger()
    conn = DjangoAMQPConnection()
    m = MyMessager(connection=conn).fetch()
    if m:
        msg = simplejson.loads(m.body)
        logger.info("Message receieved: %s" % msg.get("message"))
        m.ack()
    conn.close()


def connection_stress_test():
    message_count = 0
    discard_all()
    while True:
        send_a_message("FOOBARBAZ!!!")
        time.sleep(0.1)
        receive_a_message()
        message_count += 1
        print("Sent %d message(s)" % message_count)


def connection_stress_test_mp():
    message_count = 0
    pool = multiprocessing.Pool(10)
    discard_all()
    while True:
        pool.apply(send_a_message, ["FOOBARBAZ!!!"])
        time.sleep(0.1)
        r = pool.apply(receive_a_message)

        message_count += 1
        print("Sent %d message(s)" % message_count)


def connection_stress_test2():
    message_count = 0
    while True:
        _send2("FOOBARBAZ!!!")
        time.sleep(0.1)
        _recv2()
        message_count += 1
        print("Sent %d message(s)" % message_count)


def task_stress_test():
    task_count = 0
    while True:
        r = dmap(operator.add, [[2, 2], [4, 4], [8, 8]])
        print("[2+2, 4+4, 8+8] = %s" % r)
        task_count += 3
        print("Executed %d task(s)" % task_count)

if __name__ == "__main__":
    #connection_stress_test_mp()
    task_stress_test()
