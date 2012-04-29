from __future__ import absolute_import

import celery
import kombu
import os
import platform as _platform

from operator import add

from celery import datastructures
from celery import platforms
from celery.utils.functional import maybe_list
from celery.utils.text import pretty
from celery.utils.imports import qualname

from .defaults import find

SETTINGS_INFO = """%s %s"""

BUGREPORT_INFO = """
software -> celery:%(celery_v)s kombu:%(kombu_v)s py:%(py_v)s
platform -> system:%(system)s arch:%(arch)s imp:%(py_i)s
loader   -> %(loader)s
settings -> transport:%(transport)s results:%(results)s

%(human_settings)s

"""


class Settings(datastructures.ConfigurationView):

    @property
    def CELERY_RESULT_BACKEND(self):
        """Resolves deprecated alias ``CELERY_BACKEND``."""
        return self.first("CELERY_RESULT_BACKEND", "CELERY_BACKEND")

    @property
    def BROKER_TRANSPORT(self):
        """Resolves compat aliases :setting:`BROKER_BACKEND`
        and :setting:`CARROT_BACKEND`."""
        return self.first("BROKER_TRANSPORT",
                          "BROKER_BACKEND", "CARROT_BACKEND")

    @property
    def BROKER_BACKEND(self):
        """Deprecated compat alias to :attr:`BROKER_TRANSPORT`."""
        return self.BROKER_TRANSPORT

    @property
    def BROKER_HOST(self):
        return (os.environ.get("CELERY_BROKER_URL") or
                self.first("BROKER_URL", "BROKER_HOST"))

    def without_defaults(self):
        # the last stash is the default settings, so just skip that
        return Settings({}, self._order[:-1])

    def find_value_for_key(self, name, namespace="celery"):
        return self.get_by_parts(*self.find_option(name, namespace)[:-1])

    def find_option(self, name, namespace="celery"):
        return find(name, namespace)

    def get_by_parts(self, *parts):
        return self["_".join(filter(None, parts))]

    def humanize(self):
        return "\n".join(SETTINGS_INFO % (key + ':', pretty(value, width=50))
                    for key, value in self.without_defaults().iteritems())


class AppPickler(object):
    """Default application pickler/unpickler."""

    def __call__(self, cls, *args):
        kwargs = self.build_kwargs(*args)
        app = self.construct(cls, **kwargs)
        self.prepare(app, **kwargs)
        return app

    def prepare(self, app, **kwargs):
        app.conf.update(kwargs["changes"])

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
    return BUGREPORT_INFO % {"system": _platform.system(),
                            "arch": _platform.architecture(),
                            "py_i": platforms.pyimplementation(),
                            "celery_v": celery.__version__,
                            "kombu_v": kombu.__version__,
                            "py_v": _platform.python_version(),
                            "transport": app.conf.BROKER_TRANSPORT,
                            "results": app.conf.CELERY_RESULT_BACKEND,
                            "human_settings": app.conf.humanize(),
                            "loader": qualname(app.loader.__class__)}
