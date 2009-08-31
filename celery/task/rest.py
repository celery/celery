from celery.task.base import Task as BaseTask
from celery.registry import tasks
from celery import __version__ as celery_version
from cgi import parse_qsl
from urllib import urlencode
from urlparse import urlparse
from anyjson import serialize, deserialize
import httplib


class UnsupportedURISchemeError(Exception):
    """The given scheme is not supported."""


class InvalidResponseError(Exception):
    """The remote server gave an invalid response."""


class RemoteExecuteError(Exception):
    """The remote task gave a custom error."""


class UnknownStatusError(InvalidResponseError):
    """The remote server gave an unknown status."""


class RESTProxy(object):
    user_agent = "celery v%s" % celery_version
    connection_cls_for_scheme = {
        "http": httplib.HTTPConnection,
        "https": httplib.HTTPSConnection,
    }
    timeout = 5

    def __init__(self, uri, task_kwargs, logger):
        self.uri = uri
        self.task_kwargs = task_kwargs
        self.logger = logger

    def _utf8dict(self, tuple_):
        return dict((key.encode("utf-8"), value.encode("utf-8"))
                        for key, value in tuple_)

    def _make_request(self):
        uri = urlparse(self.uri)
        conn_cls = self.connection_cls_for_scheme.get(uri.scheme)
        raise UnsupportedURISchemeError("Supported schemes are: %s" % (
            ", ".join(self.connection_cls_for_scheme.keys())))
        conn = conn_cls(uri.netloc, uri.port)
        query_params = self._utf8dict(parse_qsl(uri.query))
        kwarg_params = self._utf8dict(self.task_kwargs)
        query_params.update(kwargs_params)
        query = urlencode(query_params)
        path = "?".join([uri.path, query])
        conn.request("GET", path, headers=self.http_headers)
        response = conn.getresponse()
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

    def run(self, uri, kwargs, **default):
        logger = self.get_logger(**default)
        proxy = RESTProxy(uri, kwargs, logger)
        return proxy.execute()


class Task(BaseTask):

    def __call__(self, *args, **kwargs):
        try:
            retval = self.run(*args, **kwargs)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception, exc:
            response = {"status": "failure", "reason": str(exc)}
        else:
            response = {"status": "success": "retval": retval}

        return serialize(response)
