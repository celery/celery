import os
import sys

from importlib import import_module

from celery.datastructures import DictAttribute
from celery.exceptions import ImproperlyConfigured
from celery.loaders.base import BaseLoader


ERROR_ENVVAR_NOT_SET = (
"""The environment variable %r is not set,
and as such the configuration could not be loaded.
Please set this variable and make it point to
a configuration module.""")


class AppLoader(BaseLoader):

    def __init__(self, *args, **kwargs):
        self._conf = {}
        super(AppLoader, self).__init__(*args, **kwargs)

    def config_from_envvar(self, variable_name, silent=False):
        module_name = os.environ.get(variable_name)
        if not module_name:
            if silent:
                return False
            raise ImproperlyConfigured(ERROR_ENVVAR_NOT_SET % (module_name, ))
        return self.config_from_object(module_name, silent=silent)

    def config_from_object(self, obj, silent=False):
        if isinstance(obj, basestring):
            try:
                obj = self.import_from_cwd(obj)
            except ImportError:
                if silent:
                    return False
                raise
        if not hasattr(obj, "__getitem__"):
            obj = DictAttribute(obj)
        self._conf = obj
        return True

    def on_worker_init(self):
        self.import_default_modules()

    def import_from_cwd(self, module, imp=import_module):
        """Import module, but make sure it finds modules
        located in the current directory.

        Modules located in the current directory has
        precedence over modules located in ``sys.path``.
        """
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

    @property
    def conf(self):
        return self._conf
