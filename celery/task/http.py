import urllib2
import warnings
try:
    from urlparse import parse_qsl
except ImportError:
    from cgi import parse_qsl
from urllib import urlencode
from urlparse import urlparse

from anyjson import deserialize

from celery import __version__ as celery_version
from celery.task.base import Task as BaseTask


class InvalidResponseError(Exception):
    """The remote server gave an invalid response."""


class RemoteExecuteError(Exception):
    """The remote task gave a custom error."""


class UnknownStatusError(InvalidResponseError):
    """The remote server gave an unknown status."""


def maybe_utf8(value):
    """Encode utf-8 value, only if the value is actually utf-8."""
    if isinstance(value, unicode):
        return value.encode("utf-8")
    return value


def utf8dict(tup):
    """With a dict's items() tuple return a new dict with any utf-8
    keys/values encoded."""
    return dict((key.encode("utf-8"), maybe_utf8(value))
                    for key, value in tup)


class MutableURL(object):
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

    def __str__(self):
        u = self.url
        query = urlencode(utf8dict(self.query.items()))
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


    query = property(_get_query, _set_query)


class HttpDispatch(object):
    """Make task HTTP request and collect the task result.

    :param url: The URL to request.
    :param method: HTTP method used. Currently supported methods are ``GET``
        and``POST``.
    :param task_kwargs: Task keyword arguments.
    :param logger: Logger used for user/system feedback.

    """
    user_agent = "celery/%s" % celery_version
    timeout = 5

    def __init__(self, url, method, task_kwargs, logger):
        self.url = url
        self.method = method
        self.task_kwargs = task_kwargs
        self.logger = logger

    def make_request(self, url, method, params):
        """Makes an HTTP request and returns the response."""
        request = urllib2.Request(url, params, headers=self.http_headers)
        request.headers.update(self.http_headers)
        response = urllib2.urlopen(request) # user catches errors.
        return response.read()

    def _dispatch_raw(self):
        """Dispatches the callback and returns the raw response text."""
        url = MutableURL(self.url)
        params = None
        if self.method == "GET":
            url.query.update(self.task_kwargs)
        elif self.method == "POST":
            params = urlencode(utf8dict(self.task_kwargs.items()))
        return self.make_request(str(url), self.method, params)

    def execute(self):
        warnings.warn(DeprecationWarning(
            "execute() has been deprecated and is scheduled for removal in \
            celery v1.2, please use dispatch() instead."))

    def dispatch(self):
        """Dispatch callback and return result."""
        response = self._dispatch()
        if not response:
            raise InvalidResponseError("Empty response")
        try:
            payload = deserialize(response)
        except ValueError, exc:
            raise InvalidResponseError(str(exc))

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
    """Task dispatching to an URL.

    :keyword url: The URL location of the HTTP callback task.
    :keyword method: Method to use when dispatching the callback. Usually
        ``GET`` or ``POST``.
    :keyword \*\*kwargs: Keyword arguments to pass on to the HTTP callback.

    .. attribute:: url

        If this is set, this is used as the default URL for requests.
        Default is to require the user of the task to supply the url as an
        argument, as this attribute is intended for subclasses.

    .. attribute:: method

        If this is set, this is the default method used for requests.
        Default is to require the user of the task to supply the method as an
        argument, as this attribute is intended for subclasses.

    """

    url = None
    method = None

    def run(self, url=None, method="GET", **kwargs):
        url = url or self.url
        method = method or self.method
        logger = self.get_logger(**kwargs)
        return HttpDispatch(url, method, kwargs, logger).execute()


class URL(MutableURL):
    """HTTP Callback URL

    Supports requesting an URL asynchronously.

    :param url: URL to request.
    :keyword dispatcher: Class used to dispatch the request.
        By default this is :class:`HttpDispatchTask`.

    """
    dispatcher = HttpDispatchTask

    def __init__(self, url, dispatcher=None):
        super(URL, self).__init__(url)
        self.dispatcher = dispatcher or self.dispatcher

    def get_async(self, **kwargs):
        return self.dispatcher.delay(str(self), "GET", **kwargs)

    def post_async(self, **kwargs):
        return self.dispatcher.delay(str(self), "POST", **kwargs)
