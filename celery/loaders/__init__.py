from celery.loaders.djangoapp import Loader as DjangoLoader
from celery.loaders.default import Loader as DefaultLoader
from django.conf import settings

"""
.. class:: Loader

The current loader class.

"""
Loader = DefaultLoader
if settings.configured:
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
