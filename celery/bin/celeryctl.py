import os
import sys
sys.path.append(os.getcwd())
django_project_dir = os.environ.get("DJANGO_PROJECT_DIR")
if django_project_dir:
    sys.path.append(django_project_dir)
from django.conf import settings
import optparse

OPTION_LIST = ()


def parse_options(arguments):
    """Parse the available options to ``celeryctl``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options, values


def celery_control(*args, **kwargs):
    print("ARGS: %s" % args)
    print("KWARGS: %s" % kwargs)
