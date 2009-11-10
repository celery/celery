from celery.task.base import Task as BaseTask
from celery.registry import tasks
from celery import __version__ as celery_version
from cgi import parse_qsl
from urllib import urlencode
from urlparse import urlparse
from anyjson import serialize, deserialize
import urllib2


class InvalidResponseError(Exception):
    """The remote server gave an invalid response."""


class RemoteExecuteError(Exception):
    """The remote task gave a custom error."""


class UnknownStatusError(InvalidResponseError):
    """The remote server gave an unknown status."""


class URL(object):
    """Object wrapping a Uniform Resource Locator.

    Supports editing the query parameter list.
    You can convert the object back to a string, the query will be
    properly urlencoded.

    Examples

        >>> url = URL("http://www.google.com:6580/foo/bar?x=3&y=4#foo")
        >>> url.query
        {'x': '3', 'y': '4'}
        >>> str(url)
        'http://www.google.com:6580/foo/bar?y=4&x=3#foo'
        >>> url.query["x"] = 10
        >>> url.query.update({"George": "Constanza"})
        >>> str(url)
        'http://www.google.com:6580/foo/bar?y=4&x=10&George=Constanza#foo'

    """

    def __init__(self, url):
        self.url = urlparse(url)
        self._query = dict(parse_qsl(self.url.query))

    def _utf8dict(self, tuple_):

        def value_encode(val):
            if isinstance(val, unicode):
                return val.encode("utf-8")
            return val

        return dict((key.encode("utf-8"), value_encode(value))
                        for key, value in tuple_)

    def __str__(self):
        u = self.url
        query = urlencode(self._utf8dict(self.query.items()))
        components = ["%s://" % u.scheme,
                      "%s" % u.netloc,
                      "%s" % u.path if u.path else "/",
                      ";%s" % u.params if u.params else None,
                      "?%s" % query if query else None,
                      "#%s" % u.fragment if u.fragment else None]
        return "".join(filter(None, components))

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, str(self))

    def _get_query(self):
        return self._query

    def _set_query(self, query):
        self._query = query

    query = property(_get_query, _set_query)


class RESTProxy(object):
    user_agent = "celery/%s" % celery_version
    timeout = 5

    def __init__(self, url, task_kwargs, logger):
        self.url = url
        self.task_kwargs = task_kwargs
        self.logger = logger

    def _create_request(self):
        url = URL(self.url)
        url.query.update(self.task_kwargs)
        req = urllib2.Request(str(url))
        req.headers.update(self.http_headers)
        return req

    def _make_request(self):
        request = self._create_request()
        opener = urllib2.build_opener()
        response = opener.open(request)
        return response.read()

    def execute(self):
        response = self._make_request()
        if not response:
            raise InvalidResponseError("Empty response")
        try:
            payload = deserialize(response)
        except ValueError, exc:
            raise InvalidResponseError(str(exc))

        # {"status": "success", "retval": 300}
        # {"status": "failure": "reason": "Invalid moon alignment."}
        status = payload["status"]
        if status == "success":
            return payload["retval"]
        elif status == "failure":
            raise RemoteExecuteError(payload["reason"])
        else:
            raise UnknownStatusError(str(status))

    @property
    def http_headers(self):
        headers = {"Content-Type": "application/json",
                   "User-Agent": self.user_agent}
        return headers


class RESTProxyTask(BaseTask):
    name = "celery.task.rest.RESTProxyTask"
    user_agent = "celery %s" % celery_version

    def run(self, url, **kwargs):
        logger = self.get_logger(**kwargs)
        proxy = RESTProxy(url, kwargs, logger)
        return proxy.execute()


def task_response(fun, *args, **kwargs):
    import sys
    try:
        sys.stderr.write("executing %s\n" % fun)
        retval = fun(*args, **kwargs)
        sys.stderr.write("got: %s\n" % retval)
    except Exception, exc:
        response = {"status": "failure", "reason": str(exc)}
    else:
        response = {"status": "success", "retval": retval}

    return serialize(response)


class Task(BaseTask):

    def __call__(self, *args, **kwargs):
        return task_response(self.run, *args, **kwargs)
