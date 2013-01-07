# -*- coding: utf-8 -*-
"""
    celery.contrib.bundles
    ~~~~~~~~~~~~~~~~~~~~~~

    Celery PyPI Bundles.

"""
from __future__ import absolute_import

from celery import VERSION
from bundle.extensions import Dist


defaults = {'author': 'Celery Project',
            'author_email': 'bundles@celeryproject.org',
            'url': 'http://celeryproject.org',
            'license': 'BSD'}
celery = Dist('celery', VERSION, **defaults)
django_celery = Dist('django-celery', VERSION, **defaults)
flask_celery = Dist('Flask-Celery', VERSION, **defaults)

bundles = [
    celery.Bundle(
        'celery-with-redis',
        'Bundle installing the dependencies for Celery and Redis',
        requires=['redis>=2.4.4'],
    ),
    celery.Bundle(
        'celery-with-mongodb',
        'Bundle installing the dependencies for Celery and MongoDB',
        requires=['pymongo'],
    ),
    celery.Bundle(
        'celery-with-couchdb',
        'Bundle installing the dependencies for Celery and CouchDB',
        requires=['couchdb'],
    ),
    celery.Bundle(
        'celery-with-beanstalk',
        'Bundle installing the dependencies for Celery and Beanstalk',
        requires=['beanstalkc'],
    ),

    django_celery.Bundle(
        'django-celery-with-redis',
        'Bundle installing the dependencies for Django-Celery and Redis',
        requires=['redis>=2.4.4'],
    ),
    django_celery.Bundle(
        'django-celery-with-mongodb',
        'Bundle installing the dependencies for Django-Celery and MongoDB',
        requires=['pymongo'],
    ),
    django_celery.Bundle(
        'django-celery-with-couchdb',
        'Bundle installing the dependencies for Django-Celery and CouchDB',
        requires=['couchdb'],
    ),
    django_celery.Bundle(
        'django-celery-with-beanstalk',
        'Bundle installing the dependencies for Django-Celery and Beanstalk',
        requires=['beanstalkc'],
    ),
]
