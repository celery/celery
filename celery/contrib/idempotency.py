"""Run a task body at most once per task id.

A task message can be delivered more than once, so a task can run twice
with the same task id.  :func:`claim_or_skip` lets the first run take a
claim, and makes every later run with the same id skip the body.

The claim is a function you supply, and it must be atomic across all
workers: it returns :const:`True` only for the one caller that created the
claim.  For example, with :pypi:`redis`:

.. code-block:: python

    def claim(key):
        return bool(redis_client.set(key, 1, nx=True))

What happens when a run stops after the claim depends on where the claim
is stored:

* In a separate store (Redis ``SET NX``, a row committed on its own), the
  body runs at most once.  If the worker is lost after the claim, no later
  run does the work.
* In the same database transaction as the side effects, a run that stops
  before the commit rolls back its claim too, so a later run does the work.
  Only the side effects inside that transaction happen exactly once.
"""
from celery.exceptions import Ignore
from celery.utils.log import get_logger

__all__ = ('claim_or_skip',)

logger = get_logger(__name__)


def claim_or_skip(task, claim, key=None):
    """Claim this run of ``task``, or raise :exc:`~celery.exceptions.Ignore`.

    Call it from a bound task, before the side effects.

    Arguments:
        task (~celery.app.task.Task): The running task (``self``).
        claim (Callable[[str], bool]): Creates the claim atomically.
            Returns :const:`True` if this call created it, and
            :const:`False` if it already existed.
        key (str): The claim key.  Defaults to the task name, task id and
            retry count, so a run started by :meth:`~celery.app.task.Task.retry`
            gets a new claim.

    Returns:
        str: The key that was claimed.

    Raises:
        ~celery.exceptions.Ignore: if the claim already exists.  The worker
            then acknowledges the message, and stores no result and
            calls no callbacks for this run, so the first run's result
            stays in place.
    """
    if key is None:
        request = task.request
        if request.id is None:
            raise RuntimeError(
                'claim_or_skip() needs a task id: call the task with '
                'delay(), apply_async() or apply(), or pass a key')
        key = f'celery-claim:{task.name}:{request.id}:{request.retries}'
    if claim(key):
        return key
    logger.info('%s[%s]: skipped, %s is already claimed',
                task.name, task.request.id, key)
    raise Ignore(f'{key} is already claimed')
