"""

celery.models has been moved to djcelery.models.

This file is deprecated and will be removed in Celery v2.1.0.

"""
import atexit

from django.core.exceptions import ImproperlyConfigured

@atexit.register
def _display_help():
    import sys

    sys.stderr.write("""

======================================================
ERROR: celery can't be added to INSTALLED_APPS anymore
======================================================

Please install the django-celery package and add:

    INSTALLED_APPS = "djcelery"

To install django-celery you can do one of the following:

* Download from PyPI:

    http://pypi.python.org/pypi/django-celery

* Install with pip:

    pip install django-celery

* Install with easy_install:

    easy_install django-celery

* Clone the development repository:

    http://github.com/ask/django-celery


If you weren't aware of this already you should read the
Celery 2.0 Changelog as well:
    http://github.com/ask/celery/tree/djangofree/Changelog

""")

raise ImproperlyConfigured("Please install django-celery")
