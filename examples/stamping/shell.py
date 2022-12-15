from time import sleep

from tasks import identity, mul, wait_for_revoke, xsum
from visitors import MonitoringIdStampingVisitor

from celery.canvas import Signature, chain, chord, group
from celery.result import AsyncResult


def create_canvas(n: int) -> Signature:
    """Creates a canvas to calculate: n * sum(1..n) * 10
    For example, if n = 3, the result is 3 * (1 + 2 + 3) * 10 = 180
    """
    canvas = chain(
        group(identity.s(i) for i in range(1, n+1)) | xsum.s(),
        chord(group(mul.s(10) for _ in range(1, n+1)), xsum.s()),
    )

    return canvas


def revoke_by_headers(result: AsyncResult, terminate: bool) -> None:
    """Revokes the last task in the workflow by its stamped header

    Arguments:
        result (AsyncResult): Can be either a frozen or a running result
        terminate (bool): If True, the revoked task will be terminated
    """
    result.revoke_by_stamped_headers({'mystamp': 'I am a stamp!'}, terminate=terminate)


def prepare_workflow() -> Signature:
    """Creates a canvas that waits "n * sum(1..n) * 10" in seconds,
    with n = 3.

    The canvas itself is stamped with a unique monitoring id stamp per task.
    The waiting task is stamped with different consistent stamp, which is used
    to revoke the task by its stamped header.
    """
    canvas = create_canvas(n=3)
    canvas = canvas | wait_for_revoke.s()
    canvas.stamp(MonitoringIdStampingVisitor())
    return canvas


def run_then_revoke():
    """Runs the workflow and lets the waiting task run for a while.
    Then, the waiting task is revoked by its stamped header.

    The expected outcome is that the canvas will be calculated to the end,
    but the waiting task will be revoked and terminated *during its run*.

    See worker logs for more details.
    """
    canvas = prepare_workflow()
    result = canvas.delay()
    print('Wait 5 seconds, then revoke the last task by its stamped header: "mystamp": "I am a stamp!"')
    sleep(5)
    print('Revoking the last task...')
    revoke_by_headers(result, terminate=True)


def revoke_then_run():
    """Revokes the waiting task by its stamped header before it runs.
    Then, run the workflow, which will not run the waiting task that was revoked.

    The expected outcome is that the canvas will be calculated to the end,
    but the waiting task will not run at all.

    See worker logs for more details.
    """
    canvas = prepare_workflow()
    result = canvas.freeze()
    revoke_by_headers(result, terminate=False)
    result = canvas.delay()
