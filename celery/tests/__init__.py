from django.db import connection

from celery.utils import noop

# FIXME This breaks the test-suite for some reason.
connection.creation.destroy_test_db = noop
