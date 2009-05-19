"""

Asynchronous result types.

"""
from celery.backends import default_backend


class BaseAsyncResult(object):
    """Base class for pending result, takes ``backend`` argument.
    
    .. attribute:: task_id

        The unique identifier for this task.

    .. attribute:: backend
       
        The task result backend used.
    
    """

    def __init__(self, task_id, backend):
        """Create a result instance.

        :param task_id: id of the task this is a result for.

        :param backend: task result backend used.

        """
        self.task_id = task_id
        self.backend = backend

    def is_done(self):
        """Returns ``True`` if the task executed successfully.
        
        :rtype: bool
        
        """
        return self.backend.is_done(self.task_id)

    def get(self):
        """Alias to :func:`wait`."""
        return self.wait()

    def wait(self, timeout=None):
        """Wait for task, and return the result when it arrives.
       
        :keyword timeout: How long to wait in seconds, before the
            operation times out.
        
        :raises celery.timer.TimeoutError: if ``timeout`` is not ``None`` and
            the result does not arrive within ``timeout`` seconds.
        
        If the remote call raised an exception then that
        exception will be re-raised.
        
        """
        return self.backend.wait_for(self.task_id, timeout=timeout)

    def ready(self):
        """Returns ``True`` if the task executed successfully, or raised
        an exception. If the task is still pending, or is waiting for retry
        then ``False`` is returned.
        
        :rtype: bool 

        """
        status = self.backend.get_status(self.task_id)
        return status != "PENDING" or status != "RETRY"

    def successful(self):
        """Alias to :func:`is_done`."""
        return self.is_done()

    def __str__(self):
        """``str(self)`` -> ``self.task_id``"""
        return self.task_id

    def __repr__(self):
        return "<AsyncResult: %s>" % self.task_id

    @property
    def result(self):
        """When the task is executed, this contains the return value.
       
        If the task resulted in failure, this will be the exception instance
        raised.
        """
        if self.status == "DONE" or self.status == "FAILURE":
            return self.backend.get_result(self.task_id)
        return None

    @property
    def status(self):
        """The current status of the task.
       
        Can be one of the following:

            *PENDING*

                The task is waiting for execution.

            *RETRY*

                The task is to be retried, possibly because of failure.

            *FAILURE*

                The task raised an exception, or has been retried more times
                than its limit. The :attr:`result` attribute contains the
                exception raised.

            *DONE*

                The task executed successfully. The :attr:`result` attribute
                contains the resulting value.

        """
        return self.backend.get_status(self.task_id)


class AsyncResult(BaseAsyncResult):
    """Pending task result using the default backend.

    .. attribute:: task_id
    
        The unique identifier for this task.

    .. attribute:: backend
    
        Instance of :class:`celery.backends.DefaultBackend`.

    """
    def __init__(self, task_id):
        super(AsyncResult, self).__init__(task_id, backend=default_backend)
