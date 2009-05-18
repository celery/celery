from celery.backends import default_backend


class BaseAsyncResult(object):
    """Base class for pending result, takes ``backend`` argument."""

    def __init__(self, task_id, backend):
        self.task_id = task_id
        self.backend = backend

    def is_done(self):
        """Returns ``True`` if the task executed successfully."""
        return self.backend.is_done(self.task_id)

    def get(self):
        """Alias to ``wait``."""
        return self.wait()

    def wait(self, timeout=None):
        """Return the result when it arrives.
        
        If timeout is not ``None`` and the result does not arrive within
        ``timeout`` seconds then ``celery.backends.base.TimeoutError`` is
        raised. If the remote call raised an exception then that exception
        will be reraised by get()."""
        return self.backend.wait_for(self.task_id, timeout=timeout)

    def ready(self):
        """Returns ``True`` if the task executed successfully, or raised
        an exception. If the task is still pending, or is waiting for retry
        then ``False`` is returned."""
        status = self.backend.get_status(self.task_id)
        return status != "PENDING" or status != "RETRY"

    def successful(self):
        """Alias to ``is_done``."""
        return self.is_done()

    def __str__(self):
        """str(self) -> self.task_id"""
        return self.task_id

    def __repr__(self):
        return "<AsyncResult: %s>" % self.task_id

    @property
    def result(self):
        """The tasks resulting value."""
        if self.status == "DONE" or self.status == "FAILURE":
            return self.backend.get_result(self.task_id)
        return None

    @property
    def status(self):
        """The current status of the task."""
        return self.backend.get_status(self.task_id)


class AsyncResult(BaseAsyncResult):
    """Pending task result using the default backend.""" 

    def __init__(self, task_id):
        super(AsyncResult, self).__init__(task_id, backend=default_backend)
