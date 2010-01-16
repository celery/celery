import os
import importlib

from celery.utils import get_full_cls_name
from celery.loaders.default import Loader as DefaultLoader
from celery.loaders.djangoapp import Loader as DjangoLoader

LOADER_CLASS_NAME = "Loader"
LOADER_ALIASES = {"django": "celery.loaders.djangoapp",
                  "default": "celery.loaders.default"}
_loader_cache = {}
_loader = None
_settings = None


def resolve_loader(loader):
    return LOADER_ALIASES.get(loader, loader)


def _get_loader_cls(loader):
    loader = resolve_loader(loader)
    loader_module = importlib.import_module(loader)
    return getattr(loader_module, LOADER_CLASS_NAME)


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
        _loader = _detect_loader()()
    return _loader


def load_settings():
    """Load the global settings object."""
    global _settings
    if _settings is None:
        _settings = current_loader().conf
    return _settings
