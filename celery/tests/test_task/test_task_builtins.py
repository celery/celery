from __future__ import absolute_import

from celery.task import backend_cleanup
from celery.tests.utils import Case


class test_backend_cleanup(Case):

    def test_run(self):
        backend_cleanup.apply()
