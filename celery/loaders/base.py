# -*- coding: utf-8 -*-
"""
    celery.loaders.base
    ~~~~~~~~~~~~~~~~~~~

    Loader base class.

"""
from __future__ import absolute_import

import anyjson
import importlib
import os
import re
import sys

from datetime import datetime

from kombu.utils import cached_property
from kombu.utils.encoding import safe_str

from celery.datastructures import DictAttribute
from celery.exceptions import ImproperlyConfigured
from celery.utils.imports import (
    import_from_cwd, symbol_by_name, NotAPackage, find_module,
)
from celery.utils.functional import maybe_list

ERROR_ENVVAR_NOT_SET = """\
The environment variable %r is not set,
and as such the configuration could not be loaded.
Please set this variable and make it point to
a configuration module."""

CONFIG_INVALID_NAME = """
Error: Module '%(module)s' doesn't exist, or it's not a valid \
Python module name.
"""

CONFIG_WITH_SUFFIX = CONFIG_INVALID_NAME + """
Did you mean '%(suggest)s'?
"""


class BaseLoader(object):
    """The base class for loaders.

    Loaders handles,

        * Reading celery client/worker configurations.

        * What happens when a task starts?
            See :meth:`on_task_init`.

        * What happens when the worker starts?
            See :meth:`on_worker_init`.

        * What happens when the worker shuts down?
            See :meth:`on_worker_shutdown`.

        * What modules are imported to find tasks?

    """
    builtin_modules = frozenset()
    configured = False
    error_envvar_not_set = ERROR_ENVVAR_NOT_SET
    override_backends = {}
    worker_initialized = False

    _conf = None

    def __init__(self, app=None, **kwargs):
        from celery.app import app_or_default
        self.app = app_or_default(app)
        self.task_modules = set()

    def now(self, utc=True):
        if utc:
            return datetime.utcnow()
        return datetime.now()

    def on_task_init(self, task_id, task):
        """This method is called before a task is executed."""
        pass

    def on_process_cleanup(self):
        """This method is called after a task is executed."""
        pass

    def on_worker_init(self):
        """This method is called when the worker (:program:`celery worker`)
        starts."""
        pass

    def on_worker_shutdown(self):
        """This method is called when the worker (:program:`celery worker`)
        shuts down."""
        pass

    def on_worker_process_init(self):
        """This method is called when a child process starts."""
        pass

    def import_task_module(self, module):
        self.task_modules.add(module)
        return self.import_from_cwd(module)

    def import_module(self, module, package=None):
        return importlib.import_module(module, package=package)

    def import_from_cwd(self, module, imp=None, package=None):
        return import_from_cwd(
            module,
            self.import_module if imp is None else imp,
            package=package,
        )

    def import_default_modules(self):
        return [
            self.import_task_module(m) for m in (
                tuple(self.builtin_modules) +
                tuple(maybe_list(self.app.conf.CELERY_IMPORTS)) +
                tuple(maybe_list(self.app.conf.CELERY_INCLUDE))
            )
        ]

    def init_worker(self):
        if not self.worker_initialized:
            self.worker_initialized = True
            self.import_default_modules()
            self.on_worker_init()

    def shutdown_worker(self):
        self.on_worker_shutdown()

    def init_worker_process(self):
        self.on_worker_process_init()

    def config_from_envvar(self, variable_name, silent=False):
        module_name = os.environ.get(variable_name)
        if not module_name:
            if silent:
                return False
            raise ImproperlyConfigured(self.error_envvar_not_set % module_name)
        return self.config_from_object(module_name, silent=silent)

    def config_from_object(self, obj, silent=False):
        if isinstance(obj, basestring):
            try:
                if '.' in obj:
                    obj = symbol_by_name(obj, imp=self.import_from_cwd)
                else:
                    obj = self.import_from_cwd(obj)
            except (ImportError, AttributeError):
                if silent:
                    return False
                raise
        if not hasattr(obj, '__getitem__'):
            obj = DictAttribute(obj)
        self._conf = obj
        return True

    def _import_config_module(self, name):
        try:
            self.find_module(name)
        except NotAPackage:
            if name.endswith('.py'):
                raise NotAPackage, NotAPackage(CONFIG_WITH_SUFFIX % {
                    'module': name, 'suggest': name[:-3]}), sys.exc_info()[2]
            raise NotAPackage, NotAPackage(
                CONFIG_INVALID_NAME % {'module': name}), sys.exc_info()[2]
        else:
            return self.import_from_cwd(name)

    def find_module(self, module):
        return find_module(module)

    def cmdline_config_parser(
            self, args, namespace='celery',
            re_type=re.compile(r'\((\w+)\)'),
            extra_types={'json': anyjson.loads},
            override_types={'tuple': 'json',
                            'list': 'json',
                            'dict': 'json'}):
        from celery.app.defaults import Option, NAMESPACES
        namespace = namespace.upper()
        typemap = dict(Option.typemap, **extra_types)

        def getarg(arg):
            """Parse a single configuration definition from
            the command line."""

            ## find key/value
            # ns.key=value|ns_key=value (case insensitive)
            key, value = arg.split('=', 1)
            key = key.upper().replace('.', '_')

            ## find namespace.
            # .key=value|_key=value expands to default namespace.
            if key[0] == '_':
                ns, key = namespace, key[1:]
            else:
                # find namespace part of key
                ns, key = key.split('_', 1)

            ns_key = (ns and ns + '_' or '') + key

            # (type)value makes cast to custom type.
            cast = re_type.match(value)
            if cast:
                type_ = cast.groups()[0]
                type_ = override_types.get(type_, type_)
                value = value[len(cast.group()):]
                value = typemap[type_](value)
            else:
                try:
                    value = NAMESPACES[ns][key].to_python(value)
                except ValueError, exc:
                    # display key name in error message.
                    raise ValueError('%r: %s' % (ns_key, exc))
            return ns_key, value
        return dict(getarg(v) for v in args)

    def mail_admins(self, subject, body, fail_silently=False,
                    sender=None, to=None, host=None, port=None,
                    user=None, password=None, timeout=None,
                    use_ssl=False, use_tls=False):
        message = self.mail.Message(sender=sender, to=to,
                                    subject=safe_str(subject),
                                    body=safe_str(body))
        mailer = self.mail.Mailer(host=host, port=port,
                                  user=user, password=password,
                                  timeout=timeout, use_ssl=use_ssl,
                                  use_tls=use_tls)
        mailer.send(message, fail_silently=fail_silently)

    def read_configuration(self):
        try:
            custom_config = os.environ['CELERY_CONFIG_MODULE']
        except KeyError:
            pass
        else:
            usercfg = self._import_config_module(custom_config)
            return DictAttribute(usercfg)
        return {}

    @property
    def conf(self):
        """Loader configuration."""
        if self._conf is None:
            self._conf = self.read_configuration()
        return self._conf

    @cached_property
    def mail(self):
        return self.import_module('celery.utils.mail')
