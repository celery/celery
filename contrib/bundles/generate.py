import os
import sys

sys.path.insert(0, os.path.abspath(
    os.path.join(__file__, os.pardir, os.pardir)))

from celery import VERSION

from bundle import Bundle

series = "{}.{}".format(*VERSION[:2])

defaults = {"version": series,
            "author": "Celery Project",
            "author_email": "bundles@celeryproject.org",
            "url": "http://celeryproject.org",
            "license": "BSD"}


bundles = [
    Bundle("celery-with-redis",
        "Bundle that installs the dependencies for Celery and Redis",
        requires=["celery>=%s,<3.0" % (series, ), "redis>=2.4.4"],
        **defaults),
]


def main():
    for bundle in bundles:
        bundle.bump_if_exists()
        print(bundle.render_readme())

if __name__ == "__main__":
    main()
