import os
import re
import sys

import anyjson

from importlib import import_module as _import_module

BUILTIN_MODULES = ["celery.task"]


class BaseLoader(object):
    """The base class for loaders.

    Loaders handles to following things:

        * Reading celery client/worker configurations.

        * What happens when a task starts?
            See :meth:`on_task_init`.

        * What happens when the worker starts?
            See :meth:`on_worker_init`.

        * What modules are imported to find tasks?

    """
    _conf_cache = None
    worker_initialized = False
    override_backends = {}
    configured = False

    def __init__(self, app=None, **kwargs):
        from celery.app import app_or_default
        self.app = app_or_default(app)

    def on_task_init(self, task_id, task):
        """This method is called before a task is executed."""
        pass

    def on_process_cleanup(self):
        """This method is called after a task is executed."""
        pass

    def on_worker_init(self):
        """This method is called when the worker (``celeryd``) starts."""
        pass

    def import_task_module(self, module):
        return self.import_from_cwd(module)

    def import_module(self, module):
        return _import_module(module)

    def import_default_modules(self):
        imports = self.conf.get("CELERY_IMPORTS") or []
        imports = set(list(imports) + BUILTIN_MODULES)
        return map(self.import_task_module, imports)

    def init_worker(self):
        if not self.worker_initialized:
            self.worker_initialized = True
            self.on_worker_init()

    def cmdline_config_parser(self, args, namespace="celery",
                re_type=re.compile(r"\((\w+)\)"),
                extra_types={"json": anyjson.deserialize},
                override_types={"tuple": "json",
                                "list": "json",
                                "dict": "json"}):
        from celery.app.defaults import Option, NAMESPACES
        namespace = namespace.upper()
        typemap = dict(Option.typemap, **extra_types)

        def getarg(arg):
            """Parse a single configuration definition from
            the command line."""

            ## find key/value
            # ns.key=value|ns_key=value (case insensitive)
            key, value = arg.replace('.', '_').split('=', 1)
            key = key.upper()

            ## find namespace.
            # .key=value|_key=value expands to default namespace.
            if key[0] == '_':
                ns, key = namespace, key[1:]
            else:
                # find namespace part of key
                ns, key = key.split('_', 1)

            ns_key = (ns and ns + "_" or "") + key

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
                    raise ValueError("%r: %s" % (ns_key, exc))
            return ns_key, value

        return dict(map(getarg, args))

    @property
    def conf(self):
        """Loader configuration."""
        if not self._conf_cache:
            self._conf_cache = self.read_configuration()
        return self._conf_cache

    def import_from_cwd(self, module, imp=None):
        """Import module, but make sure it finds modules
        located in the current directory.

        Modules located in the current directory has
        precedence over modules located in ``sys.path``.
        """
        if imp is None:
            imp = self.import_module
        cwd = os.getcwd()
        if cwd in sys.path:
            return imp(module)
        sys.path.insert(0, cwd)
        try:
            return imp(module)
        finally:
            try:
                sys.path.remove(cwd)
            except ValueError:
                pass
