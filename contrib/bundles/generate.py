import os
import sys

sys.path.insert(0, os.path.abspath(
    os.path.join(__file__, os.pardir, os.pardir)))

from celery import VERSION

from bundle import Bundle

series = "{}.{}".format(*VERSION[:2])
next_major = VERSION[0] + 1
base_fmt = "{base}>={series},<{next_major}"

defaults = {"version": series,
            "author": "Celery Project",
            "author_email": "bundles@celeryproject.org",
            "url": "http://celeryproject.org",
            "license": "BSD"}


def basereq(base):
    return base_fmt.format(base=base, series=series, next_major=next_major)


def _reqs(base, *reqs):
    return [basereq(base)] + list(reqs)


def celery_with(*reqs):
    return _reqs("celery", *reqs)


def djcelery_with(*reqs):
    return _reqs("django-celery", *reqs)


bundles = [
    Bundle("celery-with-redis",
        "Bundle that installs the dependencies for Celery and Redis",
        requires=celery_with("redis>=2.4.4"), **defaults),
    Bundle("celery-with-mongodb",
        "Bundle that installs the dependencies for Celery and MongoDB",
        requires=celery_with("pymongo"), **defaults),
    Bundle("django-celery-with-redis",
        "Bundle that installs the dependencies for Django-Celery and Redis",
        requires=djcelery_with("redis>=2.4.4"), **defaults),
    Bundle("django-celery-with-mongodb",
        "Bundle that installs the dependencies for Django-Celery and MongoDB",
        requires=djcelery_with("redis>=2.4.4"), **defaults),
    Bundle("bundle-celery",
        "Bundle that installs Celery related modules",
        requires=celery_with("setproctitle", "celerymon", "cyme",
                             "kombu-sqlalchemy", "django-kombu",
                             basereq("django-celery"),
                             basereq("Flask-Celery")), **defaults),
]


def main():
    for bundle in bundles:
        print("* Updating %s (%s)" % (bundle.name.ljust(30), bundle.version))
        bundle.register()

if __name__ == "__main__":
    main()
