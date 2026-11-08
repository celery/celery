"""Check that a task leaves the same side effects when it runs twice.

A task message can be delivered more than once: after a worker is lost,
after a visibility timeout expires, when an acknowledgement is lost, or when
the same message is published twice.  :func:`assert_idempotent` runs a task
twice with the same task id, in this process and without a broker, and
compares the side effects that the test can observe.
"""
import threading

from celery import signals, states, uuid

__all__ = ('assert_idempotent',)


def assert_idempotent(task, args=(), kwargs=None, *, observe, reset=None,
                      concurrent=True, rounds=5, timeout=10.0, **options):
    """Run ``task`` twice with one task id and compare what ``observe`` sees.

    Each run calls :meth:`~celery.app.task.Task.apply`.

    First the two runs happen one after the other, as when a message is
    delivered again after the first run finished but before it was
    acknowledged.  Then, if ``concurrent`` is true, the state is reset and
    two runs start the task body at the same time, as when two workers
    receive the same message.  The concurrent check is repeated ``rounds``
    times, with a new task id and a call to ``reset`` each time.

    Arguments:
        task (~celery.app.task.Task): The task to check.
        args (Sequence): Positional arguments for the task.
        kwargs (Mapping): Keyword arguments for the task.
        observe (Callable[[], Any]): Returns the side effects to compare,
            for example the rows in a table or the calls made to a mock.
            The values must support ``==``.
        reset (Callable[[], None]): Restores the state from before the
            first run.  The concurrent check needs it.
        concurrent (bool): Also run the two copies at the same time.
        rounds (int): How many times to repeat the concurrent check.
        timeout (float): Seconds to wait for the concurrent runs.
        **options (Any): Passed to :meth:`~celery.app.task.Task.apply`.

    Returns:
        Any: What ``observe`` returned after one run.

    Raises:
        AssertionError: if a run fails, or if running twice leaves
            different side effects than running once.

    A pass is evidence, not proof.  The concurrent runs are two threads in
    one process: they find a race between a check and an update only when
    the task body switches threads in between, which happens during I/O
    such as a database or Redis call.
    """
    if concurrent and reset is None:
        raise TypeError(
            'assert_idempotent() needs reset() for the concurrent check, '
            'or concurrent=False')
    if reset is not None:
        reset()
    task_id = uuid()
    _run(task, args, kwargs, task_id, options)
    once = observe()
    _run(task, args, kwargs, task_id, options)
    twice = observe()
    if twice != once:
        raise AssertionError(
            f'{task.name} is not idempotent: a second run with the same '
            f'task id changed the side effects.\n'
            f'after one run:  {once!r}\n'
            f'after two runs: {twice!r}')

    for _ in range(rounds if concurrent else 0):
        reset()
        _run_together(task, args, kwargs, uuid(), options, timeout)
        overlapping = observe()
        if overlapping != once:
            raise AssertionError(
                f'{task.name} is not idempotent: two runs with the same '
                f'task id at the same time changed the side effects.\n'
                f'after one run:             {once!r}\n'
                f'after two concurrent runs: {overlapping!r}')
    return once


def _run(task, args, kwargs, task_id, options):
    result = task.apply(args, kwargs, task_id=task_id, throw=False, **options)
    if result.state == states.FAILURE:
        raise AssertionError(
            f'{task.name}[{task_id}] failed: {result.result!r}'
        ) from result.result


def _run_together(task, args, kwargs, task_id, options, timeout):
    # Wait in task_prerun, so that the two task bodies start together,
    # not the two calls to apply().  Each thread waits once: a retry
    # with the same task id must not wait again.
    barrier = threading.Barrier(2, timeout=timeout)
    waited = threading.local()
    errors = []

    def at_body_start(sender=None, task_id=None, **kw):
        if task_id == run_id and not getattr(waited, 'done', False):
            waited.done = True
            barrier.wait()

    def run():
        try:
            _run(task, args, kwargs, run_id, options)
        except BaseException as exc:  # raised again in the calling thread
            errors.append(exc)

    run_id = task_id
    threads = [threading.Thread(target=run, daemon=True,
                                name=f'assert_idempotent-{i}')
               for i in range(2)]
    signals.task_prerun.connect(at_body_start, sender=task, weak=False)
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout)
    finally:
        signals.task_prerun.disconnect(at_body_start, sender=task)
    if any(thread.is_alive() for thread in threads):
        raise AssertionError(
            f'{task.name}: the concurrent runs did not finish in {timeout}s')
    if errors:
        raise errors[0]
