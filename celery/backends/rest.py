"""
    celery.backends.rest
    ~~~~~~~~~~~~~~~~~~~~

    REST result backend.

"""
from __future__ import absolute_import

__author__ = """\
Tal Liron <tal.liron@threecrickets.com>
"""

from base64 import b64encode, b64decode

from kombu.utils import cached_property
from kombu.utils.rest import rest_request, RESTError

from celery.exceptions import ImproperlyConfigured

from .base import KeyValueStoreBackend


DEFAULT_PROTOCOL = 'http'
DEFAULT_FORMAT = 'json'

TASK_RESULT_URL =        '%(base)s/task/%(task)s/result/'
TASK_RESULT_FORMAT_URL = '%(base)s/task/%(task)s/result/?format=%(format)s'


class RESTBackend(KeyValueStoreBackend):
    """
    Works with the Prudence-based RESTful messaging backend.
    
    Configure using CELERY_REST_BACKEND_SETTINGS:
    
     'protocol': 'http', 'https' (defaults to 'http')
     'format': 'json', 'base64' (defaults to 'json')
    """
    #supports_native_join = True # if we support get_many (=mget?)

    def __init__(self, expires=None, backend=None, options={}, **kwargs):
        super(RESTBackend, self).__init__(self, **kwargs)
        
        protocol = DEFAULT_PROTOCOL
        self._format = DEFAULT_FORMAT

        # Settings
        config = self.app.conf.get('CELERY_REST_BACKEND_SETTINGS', None)
        if config is not None:
            if not isinstance(config, dict):
                raise ImproperlyConfigured('REST backend settings should be grouped in a dict')
            if 'format' in config:
                self._format = config['format']
            if 'protocol' in config:
                protocol = config['protocol']
                
        self.options = options
        self.backend = backend
        self.expires = self.prepare_expires(expires, type=int)
        self._base_url = protocol + ':' + kwargs['url'][5:]

    def get(self, key):
        result = rest_request(TASK_RESULT_FORMAT_URL, base=self._base_url, task=key, format=self._format)

        print 'result.get -> %s' % result
                
        if result is None:
            return None
        result = result['result']
        if result is None:
            return None

        if self._format == 'json':
            result = self.encode(result)
        elif self._format == 'base64':
            result = b64decode(result)

        return result

    # This seems to be an optional optimization, perhaps useful for chords (see 'supports_native_join')
    #def mget(self, keys):
    #    print 'result.mget %s' % keys
    #    # TODO

    def set(self, key, value, **kwargs):
        if self._format == 'json':
            value = self.decode(value)
        elif self._format == 'base64':
            value = b64encode(value)
            
        print 'result.set %s, %s' % (key, value)

        rest_request(TASK_RESULT_FORMAT_URL, base=self._base_url, task=key, format=self._format,
            payload={'result': value})

    def delete(self, key):
        print 'result.delete %s' % key
        rest_request(TASK_RESULT_URL, base=self._base_url, task=key,
            method='DELETE')

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(dict(
            backend=self._base_url,
            expires=self.expires,
            options=self.options))
        return super(RESTBackend, self).__reduce__(args, kwargs)
