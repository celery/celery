import os
import warnings
import importlib

from carrot.utils import rpartition

from celery.utils import get_full_cls_name, first
from celery.loaders.default import Loader as DefaultLoader

from djcelery.loaders.djangoapp import Loader as DjangoLoader

_DEFAULT_LOADER_CLASS_NAME = "Loader"
LOADER_ALIASES = {"default": "celery.loaders.default.Loader",
                  "django": "djcelery.loaders.djangoapp.Loader"}
_loader_cache = {}
_loader = None
_settings = None


def resolve_loader(loader):
    loader = LOADER_ALIASES.get(loader, loader)
    loader_module_name, _, loader_cls_name = rpartition(loader, ".")

    # For backward compatibility, try to detect a valid loader name by
    # ensuring the first letter in the name is uppercase.
    # e.g. both module.Foo and module.__Foo is valid, but not module.foo.
    if not first(str.isalpha, loader_cls_name).isupper():
        warnings.warn(DeprecationWarning(
            "CELERY_LOADER now needs loader class name, e.g. %s.%s" % (
                loader, _DEFAULT_LOADER_CLASS_NAME)))
        return loader, _DEFAULT_LOADER_CLASS_NAME
    return loader_module_name, loader_cls_name


def _get_loader_cls(loader):
    loader_module_name, loader_cls_name = resolve_loader(loader)
    loader_module = importlib.import_module(loader_module_name)
    return getattr(loader_module, loader_cls_name)


def get_loader_cls(loader):
    """Get loader class by name/alias"""
    if loader not in _loader_cache:
        _loader_cache[loader] = _get_loader_cls(loader)
    return _loader_cache[loader]


def detect_loader():
    loader = os.environ.get("CELERY_LOADER")
    if loader:
        return get_loader_cls(loader)

    loader = _detect_loader()
    os.environ["CELERY_LOADER"] = get_full_cls_name(loader)

    return loader


def _detect_loader(): # pragma: no cover
    from django.conf import settings
    if settings.configured:
        return DjangoLoader
    try:
        # A settings module may be defined, but Django didn't attempt to
        # load it yet. As an alternative to calling the private _setup(),
        # we could also check whether DJANGO_SETTINGS_MODULE is set.
        settings._setup()
    except ImportError:
        if not callable(getattr(os, "fork", None)):
            # Platform doesn't support fork()
            # XXX On systems without fork, multiprocessing seems to be
            # launching the processes in some other way which does
            # not copy the memory of the parent process. This means
            # any configured env might be lost. This is a hack to make
            # it work on Windows.
            # A better way might be to use os.environ to set the currently
            # used configuration method so to propogate it to the "child"
            # processes. But this has to be experimented with.
            # [asksol/heyman]
            from django.core.management import setup_environ
            try:
                settings_mod = os.environ.get("DJANGO_SETTINGS_MODULE",
                                                "settings")
                project_settings = __import__(settings_mod, {}, {}, [''])
                setup_environ(project_settings)
                return DjangoLoader
            except ImportError:
                pass
    else:
        return DjangoLoader

    return DefaultLoader


def current_loader():
    """Detect and return the current loader."""
    global _loader
    if _loader is None:
        _loader = detect_loader()()
    return _loader


def load_settings():
    """Load the global settings object."""
    global _settings
    if _settings is None:
        _settings = current_loader().conf
    return _settings
