from celery.loaders.djangoapp import Loader as DjangoLoader
from celery.loaders.default import Loader as DefaultLoader
from django.conf import settings
from django.core.management import setup_environ

"""
.. class:: Loader

The current loader class.

"""
Loader = DefaultLoader
if settings.configured:
    Loader = DjangoLoader
else:
    # We might still be running celery with django, because worker processes
    # spawned with celery running through manage.py, will not have had their
    # django environment set up
    try:
        # If we can import 'settings', assume we're running celery with django
        import settings as project_settings
        setup_environ(project_settings)
        Loader = DjangoLoader
    except ImportError:
        pass

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
