import os
import sys

from celery import VERSION
from bundle import Bundle
from bundle.extensions import Dist


defaults = {"author": "Celery Project",
            "author_email": "bundles@celeryproject.org",
            "url": "http://celeryproject.org",
            "license": "BSD"}
celery = Dist("celery", VERSION, **defaults)
django_celery = Dist("django-celery", VERSION, **defaults)
flask_celery = Dist("Flask-Celery", VERSION, **defaults)

bundles = [
    celery.Bundle("celery-with-redis",
        "Bundle installing the dependencies for Celery and Redis",
        requires=["redis>=2.4.4"]),
    celery.Bundle("celery-with-mongodb",
        "Bundle installing the dependencies for Celery and MongoDB",
        requires=["pymongo"]),
    django_celery.Bundle("django-celery-with-redis",
        "Bundle that installs the dependencies for Django-Celery and Redis",
        requires=["redis>=2.4.4"]),
    django_celery.Bundle("django-celery-with-mongodb",
        "Bundle that installs the dependencies for Django-Celery and MongoDB",
        requires=["redis>=2.4.4"]),
    celery.Bundle("bundle-celery",
        "Bundle that installs Celery related modules",
        requires=[django_celery, flask_celery,
                  "django", "setproctitle", "celerymon",
                  "cyme", "kombu-sqlalchemy", "django-kombu"]),
]
