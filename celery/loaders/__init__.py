import os

from django.conf import settings
from django.core.management import setup_environ

from celery.loaders.default import Loader as DefaultLoader
from celery.loaders.djangoapp import Loader as DjangoLoader

"""
.. class:: Loader

The current loader class.

"""
Loader = DefaultLoader
if settings.configured:
    Loader = DjangoLoader
else:
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
            # not copy the memory of the parent process. This means that any
            # configured env might be lost. This is a hack to make it work
            # on Windows.
            # A better way might be to use os.environ to set the currently
            # used configuration method so to propogate it to the "child"
            # processes. But this has to be experimented with.
            # [asksol/heyman]
            try:
                settings_mod = os.environ.get("DJANGO_SETTINGS_MODULE",
                                              "settings")
                project_settings = __import__(settings_mod, {}, {}, [''])
                setup_environ(project_settings)
                Loader = DjangoLoader
            except ImportError:
                pass
    else:
        Loader = DjangoLoader

"""
.. data:: current_loader

The current loader instance.

"""
current_loader = Loader()


"""
.. data:: settings

The global settings object.

"""
settings = current_loader.conf
