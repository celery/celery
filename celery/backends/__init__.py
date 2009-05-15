from functools import partial
from django.conf import settings
import sys

DEFAULT_BACKEND = "database"
CELERY_BACKEND = getattr(settings, "CELERY_BACKEND", DEFAULT_BACKEND)


class BaseJob(object):

    def __init__(self, task_id, backend):
        self.task_id = task_id
        self.backend = backend

    def __str__(self):
        return self.task_id

    def __repr__(self):
        return "<Job: %s>" % self.task_id

    def is_done(self):
        return self.backend.is_done(self.task_id)

    def wait_for(self):
        return self.backend.wait_for(self.task_id)

    @property
    def result(self):
        if self.status == "DONE":
            return self.backend.get_result(self.task_id)
        return None

    @property
    def status(self):
        return self.backend.get_status(self.task_id)



def get_backend_cls(backend):
    if backend.find(".") == -1:
        backend = "celery.backends.%s" % backend
    __import__(backend)
    backend_module = sys.modules[backend]
    return getattr(backend_module, 'Backend')

get_default_backend_cls = partial(get_backend_cls, CELERY_BACKEND)
DefaultBackend = get_default_backend_cls()
default_backend = DefaultBackend()

class Job(BaseJob):
    
    def __init__(self, task_id):
        super(Job, self).__init__(task_id, backend=default_backend)


