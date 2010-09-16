import os

from celery.utils import get_cls_by_name

LOADER_ALIASES = {"app": "celery.loaders.app.AppLoader",
                  "default": "celery.loaders.default.Loader",
                  "django": "djcelery.loaders.DjangoLoader"}
_loader = None
_settings = None


def get_loader_cls(loader):
    """Get loader class by name/alias"""
    return get_cls_by_name(loader, LOADER_ALIASES)


def setup_loader():
    return get_loader_cls(os.environ.setdefault("CELERY_LOADER", "default"))()


def current_loader():
    """Detect and return the current loader."""
    global _loader
    if _loader is None:
        _loader = setup_loader()
    return _loader


def load_settings():
    """Load the global settings object."""
    global _settings
    if _settings is None:
        _settings = current_loader().conf
    return _settings
