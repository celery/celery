# -*- coding: utf-8 -*-
"""
    celery.app.utils
    ~~~~~~~~~~~~~~~~

    App utilities: Compat settings, bugreport tool, pickling apps.

"""
from __future__ import absolute_import

import os
import platform as _platform
import types

from celery import platforms
from celery.datastructures import ConfigurationView, DictAttribute
from celery.utils.text import pretty
from celery.utils.imports import qualname

from .defaults import find

#: Format used to generate bugreport information.
BUGREPORT_INFO = """
software -> celery:%(celery_v)s kombu:%(kombu_v)s py:%(py_v)s
            billiard:%(billiard_v)s %(driver_v)s
platform -> system:%(system)s arch:%(arch)s imp:%(py_i)s
loader   -> %(loader)s
settings -> transport:%(transport)s results:%(results)s

%(human_settings)s
"""


class Settings(ConfigurationView):
    """Celery settings object."""

    @property
    def CELERY_RESULT_BACKEND(self):
        return self.first('CELERY_RESULT_BACKEND', 'CELERY_BACKEND')

    @property
    def BROKER_TRANSPORT(self):
        return self.first('BROKER_TRANSPORT',
                          'BROKER_BACKEND', 'CARROT_BACKEND')

    @property
    def BROKER_BACKEND(self):
        """Deprecated compat alias to :attr:`BROKER_TRANSPORT`."""
        return self.BROKER_TRANSPORT

    @property
    def BROKER_HOST(self):
        return (os.environ.get('CELERY_BROKER_URL') or
                self.first('BROKER_URL', 'BROKER_HOST'))

    @property
    def CELERY_TIMEZONE(self):
        # this way we also support django's time zone.
        return self.first('CELERY_TIMEZONE', 'TIME_ZONE')

    def without_defaults(self):
        """Returns the current configuration, but without defaults."""
        # the last stash is the default settings, so just skip that
        return Settings({}, self._order[:-1])

    def _pickleable_changes(self):
        # attempt to include keys from configuration modules,
        # to work with multiprocessing execv/fork emulation.
        # see note at celery.app.base:Celery.__reduce_args__.
        R = {}
        for d in reversed(self._order[:-1]):
            if isinstance(d, DictAttribute):
                d = object.__getattribute__(d, 'obj')
                if isinstance(d, types.ModuleType):
                    d = dict((k, v) for k, v in vars(d).iteritems()
                             if not k.startswith('__') and k.isupper())
            R.update(d)
        return R

    def find_option(self, name, namespace='celery'):
        """Search for option by name.

        Will return ``(namespace, option_name, Option)`` tuple, e.g.::

            >>> celery.conf.find_option('disable_rate_limits')
            ('CELERY', 'DISABLE_RATE_LIMITS',
             <Option: type->bool default->False>))

        :param name: Name of option, cannot be partial.
        :keyword namespace: Preferred namespace (``CELERY`` by default).

        """
        return find(name, namespace)

    def find_value_for_key(self, name, namespace='celery'):
        """Shortcut to ``get_by_parts(*find_option(name)[:-1])``"""
        return self.get_by_parts(*self.find_option(name, namespace)[:-1])

    def get_by_parts(self, *parts):
        """Returns the current value for setting specified as a path.

        Example::

            >>> celery.conf.get_by_parts('CELERY', 'DISABLE_RATE_LIMITS')
            False

        """
        return self['_'.join(filter(None, parts))]

    def humanize(self):
        """Returns a human readable string showing changes to the
        configuration."""
        return '\n'.join(
            '%s %s' % (key + ':', pretty(value, width=50))
            for key, value in self.without_defaults().iteritems())


class AppPickler(object):
    """Default application pickler/unpickler."""

    def __call__(self, cls, *args):
        kwargs = self.build_kwargs(*args)
        app = self.construct(cls, **kwargs)
        self.prepare(app, **kwargs)
        return app

    def prepare(self, app, **kwargs):
        app.conf.update(kwargs['changes'])

    def build_kwargs(self, *args):
        return self.build_standard_kwargs(*args)

    def build_standard_kwargs(self, main, changes, loader, backend, amqp,
                              events, log, control, accept_magic_kwargs):
        return dict(main=main, loader=loader, backend=backend, amqp=amqp,
                    changes=changes, events=events, log=log, control=control,
                    set_as_current=False,
                    accept_magic_kwargs=accept_magic_kwargs)

    def construct(self, cls, **kwargs):
        return cls(**kwargs)


def _unpickle_app(cls, pickler, *args):
    return pickler()(cls, *args)


def bugreport(app):
    """Returns a string containing information useful in bug reports."""
    import billiard
    import celery
    import kombu

    try:
        conn = app.connection()
        driver_v = '%s:%s' % (conn.transport.driver_name,
                              conn.transport.driver_version())
        transport = conn.transport_cls
    except Exception:
        transport = driver_v = ''

    return BUGREPORT_INFO % {
        'system': _platform.system(),
        'arch': ', '.join(filter(None, _platform.architecture())),
        'py_i': platforms.pyimplementation(),
        'celery_v': celery.VERSION_BANNER,
        'kombu_v': kombu.__version__,
        'billiard_v': billiard.__version__,
        'py_v': _platform.python_version(),
        'driver_v': driver_v,
        'transport': transport,
        'results': app.conf.CELERY_RESULT_BACKEND or 'disabled',
        'human_settings': app.conf.humanize(),
        'loader': qualname(app.loader.__class__),
    }
