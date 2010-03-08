# -*- coding: utf-8 -*-
from __future__ import generators

import sys
import logging
import unittest
from urllib import addinfourl
try:
    from contextlib import contextmanager
except ImportError:
    from celery.tests.utils import fallback_contextmanager as contextmanager
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from billiard.utils.functional import wraps
from anyjson import serialize

from celery.task import http

from celery.tests.utils import eager_tasks, execute_context


@contextmanager
def mock_urlopen(response_method):

    import urllib2
    urlopen = urllib2.urlopen

    @wraps(urlopen)
    def _mocked(url, *args, **kwargs):
        response_data, headers = response_method(url)
        return addinfourl(StringIO(response_data), headers, url)

    urllib2.urlopen = _mocked

    yield True

    urllib2.urlopen = urlopen


def _response(res):
    return lambda r: (res, [])


def success_response(value):
    return _response(serialize({"status": "success", "retval": value}))


def fail_response(reason):
    return _response(serialize({"status": "failure", "reason": reason}))


def unknown_response():
    return _response(serialize({"status": "u.u.u.u", "retval": True}))


class TestEncodings(unittest.TestCase):

    def test_utf8dict(self):
        d = {u"følelser ær langé": u"ærbadægzaååÆØÅ",
              "foobar".encode("utf-8"): "xuzzybaz".encode("utf-8")}

        for key, value in http.utf8dict(d.items()).items():
            self.assertTrue(isinstance(key, str))
            self.assertTrue(isinstance(value, str))


class TestMutableURL(unittest.TestCase):

    def test_url_query(self):
        url = http.MutableURL("http://example.com?x=10&y=20&z=Foo")
        self.assertEquals(url.query.get("x"), "10")
        self.assertEquals(url.query.get("y"), "20")
        self.assertEquals(url.query.get("z"), "Foo")
        url.query["name"] = "George"
        url = http.MutableURL(str(url))
        self.assertEquals(url.query.get("x"), "10")
        self.assertEquals(url.query.get("y"), "20")
        self.assertEquals(url.query.get("z"), "Foo")
        self.assertEquals(url.query.get("name"), "George")

    def test_url_keeps_everything(self):
        url = "https://e.com:808/foo/bar#zeta?x=10&y=20"
        url = http.MutableURL(url)

        self.assertEquals(str(url).split("?")[0],
            "https://e.com:808/foo/bar#zeta")

    def test___repr__(self):
        url = http.MutableURL("http://e.com/foo/bar")
        self.assertTrue(repr(url).startswith("<MutableURL: http://e.com"))

    def test_set_query(self):
        url = http.MutableURL("http://e.com/foo/bar/?x=10")
        url.query = {"zzz": "xxx"}
        url = http.MutableURL(str(url))
        self.assertEquals(url.query, {"zzz": "xxx"})


class TestHttpDispatch(unittest.TestCase):

    def test_dispatch_success(self):
        logger = logging.getLogger("celery.unittest")

        def with_mock_urlopen(_val):
            d = http.HttpDispatch("http://example.com/mul", "GET", {
                                    "x": 10, "y": 10}, logger)
            self.assertEquals(d.dispatch(), 100)

        context = mock_urlopen(success_response(100))
        execute_context(context, with_mock_urlopen)

    def test_dispatch_failure(self):
        logger = logging.getLogger("celery.unittest")

        def with_mock_urlopen(_val):
            d = http.HttpDispatch("http://example.com/mul", "GET", {
                                    "x": 10, "y": 10}, logger)
            self.assertRaises(http.RemoteExecuteError, d.dispatch)

        context = mock_urlopen(fail_response("Invalid moon alignment"))
        execute_context(context, with_mock_urlopen)

    def test_dispatch_empty_response(self):
        logger = logging.getLogger("celery.unittest")

        def with_mock_urlopen(_val):
            d = http.HttpDispatch("http://example.com/mul", "GET", {
                                    "x": 10, "y": 10}, logger)
            self.assertRaises(http.InvalidResponseError, d.dispatch)

        context = mock_urlopen(_response(""))
        execute_context(context, with_mock_urlopen)

    def test_dispatch_non_json(self):
        logger = logging.getLogger("celery.unittest")

        def with_mock_urlopen(_val):
            d = http.HttpDispatch("http://example.com/mul", "GET", {
                                    "x": 10, "y": 10}, logger)
            self.assertRaises(http.InvalidResponseError, d.dispatch)

        context = mock_urlopen(_response("{'#{:'''"))
        execute_context(context, with_mock_urlopen)

    def test_dispatch_unknown_status(self):
        logger = logging.getLogger("celery.unittest")

        def with_mock_urlopen(_val):
            d = http.HttpDispatch("http://example.com/mul", "GET", {
                                    "x": 10, "y": 10}, logger)
            self.assertRaises(http.UnknownStatusError, d.dispatch)

        context = mock_urlopen(unknown_response())
        execute_context(context, with_mock_urlopen)

    def test_dispatch_POST(self):
        logger = logging.getLogger("celery.unittest")

        def with_mock_urlopen(_val):
            d = http.HttpDispatch("http://example.com/mul", "POST", {
                                    "x": 10, "y": 10}, logger)
            self.assertEquals(d.dispatch(), 100)

        context = mock_urlopen(success_response(100))
        execute_context(context, with_mock_urlopen)


class TestURL(unittest.TestCase):

    def test_URL_get_async(self):
        def with_eager_tasks(_val):

            def with_mock_urlopen(_val):
                d = http.URL("http://example.com/mul").get_async(x=10, y=10)
                self.assertEquals(d.get(), 100)

            context = mock_urlopen(success_response(100))
            execute_context(context, with_mock_urlopen)

        execute_context(eager_tasks(), with_eager_tasks)

    def test_URL_post_async(self):
        def with_eager_tasks(_val):

            def with_mock_urlopen(_val):
                d = http.URL("http://example.com/mul").post_async(x=10, y=10)
                self.assertEquals(d.get(), 100)

            context = mock_urlopen(success_response(100))
            execute_context(context, with_mock_urlopen)

        execute_context(eager_tasks(), with_eager_tasks)
