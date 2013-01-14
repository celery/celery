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

from billiard import forking as _forking

from celery import platforms
from celery.five import items
from celery.datastructures import ConfigurationView, DictAttribute
from celery.utils.text import pretty
from celery.utils.imports import qualname

from .defaults import find

#: Format used to generate bugreport information.
BUGREPORT_INFO = """
software -> celery:{celery_v} kombu:{kombu_v} py:{py_v}
            billiard:{billiard_v} {driver_v}
platform -> system:{system} arch:{arch} imp:{py_i}
loader   -> {loader}
settings -> transport:{transport} results:{results}

{human_settings}
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

    def _prepare_pickleable_changes(self):
        # attempt to include keys from configuration modules,
        # to work with multiprocessing execv/fork emulation.
        # This is necessary when multiprocessing execv/fork emulation
        # is enabled.  There may be a better way to do this, but attempts
        # at forcing the subprocess to import the modules did not work out,
        # because of some sys.path problem.  More at Issue #1126.
        if _forking._forking_is_enabled:
            return self.changes
        R = {}
        for d in reversed(self._order[:-1]):
            if isinstance(d, DictAttribute):
                d = object.__getattribute__(d, 'obj')
                if isinstance(d, types.ModuleType):
                    d = dict((k, v) for k, v in items(vars(d))
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
        return self['_'.join(part for part in parts if part)]

    def humanize(self):
        """Returns a human readable string showing changes to the
        configuration."""
        return '\n'.join(
            '{0}: {1}'.format(key, pretty(value, width=50))
            for key, value in items(self.without_defaults()))


class AppPickler(object):
    """Old application pickler/unpickler (<= 3.0)."""

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
    """Rebuild app for versions 2.5+"""
    return pickler()(cls, *args)


def _unpickle_app_v2(cls, kwargs):
    """Rebuild app for versions 3.1+"""
    kwargs['set_as_current'] = False
    return cls(**kwargs)


def bugreport(app):
    """Returns a string containing information useful in bug reports."""
    import billiard
    import celery
    import kombu

    try:
        conn = app.connection()
        driver_v = '{0}:{1}'.format(conn.transport.driver_name,
                                    conn.transport.driver_version())
        transport = conn.transport_cls
    except Exception:
        transport = driver_v = ''

    return BUGREPORT_INFO.format(
        system=_platform.system(),
        arch=', '.join(x for x in _platform.architecture() if x),
        py_i=platforms.pyimplementation(),
        celery_v=celery.VERSION_BANNER,
        kombu_v=kombu.__version__,
        billiard_v=billiard.__version__,
        py_v=_platform.python_version(),
        driver_v=driver_v,
        transport=transport,
        results=app.conf.CELERY_RESULT_BACKEND or 'disabled',
        human_settings=app.conf.humanize(),
        loader=qualname(app.loader.__class__),
    )
