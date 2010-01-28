import urllib2
try:
    from urlparse import parse_qsl
except ImportError:
    from cgi import parse_qsl
from urllib import urlencode
from urlparse import urlparse

from anyjson import serialize, deserialize
from billiard.utils.functional import wraps

from celery import __version__ as celery_version
from celery.task.base import Task as BaseTask


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
                      u.path and "%s" % u.path or "/",
                      u.params and ";%s" % u.params or None,
                      query and "?%s" % query or None,
                      u.fragment and "#%s" % u.fragment or None]
        return "".join(filter(None, components))

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, str(self))

    def _get_query(self):
        return self._query

    def _set_query(self, query):
        self._query = query

    def get_async(self, **kwargs):
        return HttpDispatchTask.delay(str(self), "GET", **kwargs)

    def post_async(self, **kwargs):
        return HttpDispatchTask.delay(str(self), "POST", **kwargs)

    query = property(_get_query, _set_query)


class HttpDispatch(object):
    user_agent = "celery/%s" % celery_version
    timeout = 5

    def __init__(self, url, method, task_kwargs, logger):
        self.url = url
        self.method = method
        self.task_kwargs = task_kwargs
        self.logger = logger

    def _create_request(self):
        url = URL(self.url)
        url.query.update(self.task_kwargs)
        print("URL: %s" % str(url))
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
            raise RemoteExecuteError(payload.get("reason"))
        else:
            raise UnknownStatusError(str(status))

    @property
    def http_headers(self):
        headers = {"Content-Type": "application/json",
                   "User-Agent": self.user_agent}
        return headers


class HttpDispatchTask(BaseTask):

    def run(self, url, method="GET", **kwargs):
        logger = self.get_logger(**kwargs)
        return HttpDispatch(url, method, kwargs, logger).execute()


def http_task_response(fun, *args, **kwargs):
    try:
        retval = fun(*args, **kwargs)
    except Exception, exc:
        response = {"status": "failure", "reason": str(exc)}
    else:
        response = {"status": "success", "retval": retval}

    return serialize(response)


class Task(BaseTask):
    abstract = True

    def __call__(self, *args, **kwargs):
        return http_task_response(self.run, *args, **kwargs)
