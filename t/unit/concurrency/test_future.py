
from __future__ import absolute_import, unicode_literals

from case import skip
from tornado import gen, httpclient, ioloop

from celery.concurrency.future import (
    defaultFutureExecutor, eventletFutureExecutor, geventFutureExecutor)


@skip.if_pypy()
class test_FutureExecutor:

    # start tornado hub thread before running test to avoid any linger threads
    tornado_hub = defaultFutureExecutor.get_hub(
        io_loop=ioloop.IOLoop.instance())

    @gen.coroutine
    def foo(self, count):
        res = yield self.bar(count)
        raise gen.Return(res + 1)

    @gen.coroutine
    def bar(self, count):
        raise gen.Return(count)

    @gen.coroutine
    def foo_http(self, url):
        res = yield httpclient.AsyncHTTPClient(
            io_loop=self.tornado_hub.io_loop).fetch(url)
        raise gen.Return(res.body)

    @gen.coroutine
    def stop_httpserver(self, httpserver):
        httpserver.stop()

    def test_default_executor(self):
        count = 1

        executor = defaultFutureExecutor
        ret = executor.apply_future(self.foo, count)

        assert ret == count + 1

    def test_default_executor_http(self, httpserver):
        content = b'Hello world!'
        httpserver.serve_content(content=content)

        from threading import Event
        from time import sleep

        finish_event = Event()

        def task():
            executor = defaultFutureExecutor
            executor.apply_future(
                self.foo_http, httpserver.url, callback=finish_cb)

        def finish_cb(f):
            assert f.result() == content
            finish_event.set()

        task()
        finish_event.wait()
        httpserver.stop()
        sleep(2)  # make sure httpserver thread exited

    def test_gevent_executor(self):
        from gevent import sleep, spawn_raw
        from gevent.event import Event

        count = 1
        finish_event = Event()

        def task():
            executor = geventFutureExecutor
            ret = executor.apply_future(self.foo, count)

            assert ret == count + 1

            finish_event.set()

        spawn_raw(task)
        while not finish_event.is_set():
            sleep(0.02)

    def test_gevent_executor_http(self, httpserver):
        from gevent import sleep, spawn_raw
        from gevent.event import Event

        content = b'Hello world!'
        httpserver.serve_content(content=content)
        finish_event = Event()

        def task():
            executor = geventFutureExecutor
            ret = executor.apply_future(self.foo_http, httpserver.url)

            assert ret == content

            executor.apply_future(self.stop_httpserver, httpserver)

            finish_event.set()

        spawn_raw(task)
        while not finish_event.is_set():
            sleep(0.02)

    def test_eventlet_executor(self):
        from eventlet import Event, sleep, spawn_n

        count = 1
        finish_event = Event()

        def task():
            executor = eventletFutureExecutor
            ret = executor.apply_future(self.foo, count)

            assert ret == count + 1

            finish_event.send()

        spawn_n(task)
        while not finish_event.ready():
            sleep(0.02)

    def test_eventlet_executor_http(self, httpserver):
        from eventlet import Event, sleep, spawn_n

        content = b'Hello world!'
        httpserver.serve_content(content=content)
        finish_event = Event()

        def task():
            executor = eventletFutureExecutor
            ret = executor.apply_future(self.foo_http, httpserver.url)

            assert ret == content

            executor.apply_future(self.stop_httpserver, httpserver)

            finish_event.send()

        spawn_n(task)
        while not finish_event.ready():
            sleep(0.02)
