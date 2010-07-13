import atexit
import logging
import os
import signal
import socket
import sys
import traceback
import unittest2 as unittest

from itertools import count

from celery.task.control import broadcast, ping
from celery.utils import get_full_cls_name

HOSTNAME = socket.gethostname()

def say(msg):
    sys.stderr.write("%s\n" % msg)

def flatten_response(response):
    flat = {}
    for item in response:
        flat.update(item)
    return flat

class Worker(object):
    started = False
    next_worker_id = count(1).next
    _shutdown_called = False

    def __init__(self, hostname, loglevel="error"):
        self.hostname = hostname
        self.loglevel = loglevel

    def start(self):
        if not self.started:
            self._fork_and_exec()
            self.started = True

    def _fork_and_exec(self):
        pid = os.fork()
        if pid == 0:
            os.execv(sys.executable,
                    [sys.executable] + ["-m", "celery.bin.celeryd",
                                        "-l", self.loglevel,
                                        "-n", self.hostname])
            os.exit()
        self.pid = pid

    def is_alive(self, timeout=1):
        r = ping(destination=[self.hostname],
                 timeout=timeout)
        return self.hostname in flatten_response(r)

    def wait_until_started(self, timeout=10, interval=0.2):
        for iteration in count(0):
            if iteration * interval >= timeout:
                raise Exception(
                        "Worker won't start (after %s secs.)" % timeout)
            if self.is_alive(interval):
                break
        say("--WORKER %s IS ONLINE--" % self.hostname)

    def ensure_shutdown(self, timeout=10, interval=0.5):
        os.kill(self.pid, signal.SIGTERM)
        for iteration in count(0):
            if iteration * interval >= timeout:
                raise Exception(
                        "Worker won't shutdown (after %s secs.)" % timeout)
            broadcast("shutdown", destination=[self.hostname])
            if not self.is_alive(interval):
                break
        say("--WORKER %s IS SHUTDOWN--" % self.hostname)
        self._shutdown_called = True

    def ensure_started(self):
        self.start()
        self.wait_until_started()

    @classmethod
    def managed(cls, hostname=None, caller=None):
        hostname = hostname or socket.gethostname()
        if caller:
            hostname = ".".join([get_full_cls_name(caller), hostname])
        else:
            hostname += str(cls.next_worker_id())
        worker = cls(hostname)
        worker.ensure_started()
        stack = traceback.format_stack()

        @atexit.register
        def _ensure_shutdown_once():
            if not worker._shutdown_called:
                say("-- Found worker not stopped at shutdown: %s\n%s" % (
                        worker.hostname,
                        "\n".join(stack)))
                worker.ensure_shutdown()

        return worker


class WorkerCase(unittest.TestCase):
    hostname = HOSTNAME
    worker = None

    @classmethod
    def setUpClass(cls):
        logging.getLogger("amqplib").setLevel(logging.ERROR)
        cls.worker = Worker.managed(cls.hostname, caller=cls)

    @classmethod
    def tearDownClass(cls):
        cls.worker.ensure_shutdown()

    def assertWorkerAlive(self, timeout=1):
        self.assertTrue(self.worker.is_alive)

    def my_response(self, response):
        return flatten_response(response)[self.worker.hostname]

