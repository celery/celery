"""
Celery's contract with its worker, checked with due-work-harness.

due-work-harness (https://github.com/gigaverse-app/due-work-harness) is a pytest
plugin that checks background work is neither lost nor run twice. A contract
names the guarantees a system offers and binds each to real code; the harness
then generates the test cases.

This contract binds Celery's worker itself, through ``worker_contract`` from
the harness's Celery worker integration. One task, ``send_message``, with a
link (``announce``) and an error callback (``report_failure``), goes through a
real ``celery worker`` with one prefork child, run as a child process of the
test. The harness fails it at each of Celery's own stages:

* the pool child dies at ``task_prerun``, at ``mark_as_done`` (the body ran and
  the link was published, the result not stored yet), and at ``task_postrun``
  (the result stored as SUCCESS, the parent not told);
* the task's ``on_success`` hook raises, and the broker refuses the link.

Recovery is the worker starting again. Each history must end where normal
operation does: SUCCESS, the message sent once, the link announcing it once,
no error callback. What diverges is declared as one gap, a strict xfail, and
``FINDINGS`` pins what every history leaves, in the same run as the verdict.

This directory is not part of the integration suite, which shares one session
worker: its tests start their own workers. Run it (Python 3.12 or later, and a
Redis on localhost)::

    pip install -e . -r requirements/test.txt -r requirements/test-due-work.txt
    pytest t/due_work -p no:cacheprovider
"""

import redis
from due_work_harness import Findings, due_work_contract_suite
from due_work_harness.integrations.celery_worker import worker_contract, worker_history

from celery.result import AsyncResult

from . import app as due_work

MESSAGE = 'your order has shipped'


def send():
    # ARRANGE: an empty broker, backend and record; then the task, with its link and error callback.
    for url in (due_work.BROKER, due_work.BACKEND):
        redis.Redis.from_url(url).flushdb()
    due_work.RECORDS.flushdb()
    return due_work.send_message.apply_async(
        (MESSAGE,), link=due_work.announce.s(), link_error=due_work.report_failure.s()
    ).id


def what_happened(task_id):
    # OBSERVE: the task's recorded state, how many times the message left, and what the callbacks announced.
    return (
        AsyncResult(task_id, app=due_work.app).state,
        due_work.RECORDS.llen('sent'),
        # Sorted: the link and the error callback run in different processes, in either order.
        tuple(sorted(entry.decode() for entry in due_work.RECORDS.lrange('announced', 0, -1))),
    )


def settled(task_id):
    return AsyncResult(task_id, app=due_work.app).ready()


SENT = f'sent {MESSAGE!r}'

# What each failure leaves after the worker starts again, pinned in the same run as the
# verdict; every history not listed reaches normal operation. A change in the worker moves
# an entry, and the case names it.
FINDINGS = {
    # The child died as the task started: the message is never sent, the error callback fires, and
    # nothing runs the task again (task_reject_on_worker_lost is off by default).
    'died at task_prerun': ('FAILURE', 0, ('failed: WorkerLostError',)),
    # FINDING: the message was sent and the link announced it; the task is recorded FAILURE and the
    # error callback announces a failure too.
    'died at mark_as_done': ('FAILURE', 1, ('failed: WorkerLostError', SENT)),
    # FINDING: SUCCESS was stored; the parent, told the child was lost, fires the error callback anyway.
    'died at task_postrun': ('SUCCESS', 1, ('failed: WorkerLostError', SENT)),
    # FINDING: SUCCESS was stored; the raising hook sends the task down the failure path.
    "the task's on_success hook raised": ('SUCCESS', 1, ('failed: ReceiverFailed', SENT)),
    # FINDING: the message was sent; the refused link records FAILURE, and the message is acknowledged,
    # so the link is never published again.
    "the broker refused the task's link": ('FAILURE', 1, ('failed: OperationalError',)),
}


SEND_MESSAGE = worker_history(
    name='a worker runs send_message',
    app='t.due_work.app:app',
    task=due_work.send_message.name,
    send=send,
    observe=what_happened,
    initial=('PENDING', 0, ()),
    settled=settled,
    findings=Findings(('SUCCESS', 1, (SENT,)), FINDINGS),
)

CONTRACT = worker_contract(
    name='celery: the worker running a task',
    history=SEND_MESSAGE,
    gap=(
        'once the result is stored, or the link published, a failure still runs the failure path: a pool child '
        'lost after SUCCESS is stored, or an on_success hook that raises, fires the error callback beside the '
        'link; a child lost after the link, before the store, records FAILURE for a task whose effect and link '
        'happened; and a link the broker refuses records FAILURE and acknowledges the message, so the link is '
        'never sent (https://github.com/celery/celery/issues/10724, https://github.com/celery/celery/issues/10725). '
        'FINDINGS pins each history'
    ),
)


# This is where the magic happens. The class is empty on purpose: the decorator reads CONTRACT
# and generates its tests, bound to Celery's real worker. No test case is written by hand; this
# file supplies only the task and how to see what it did, and the stages where the worker can
# fail come from the harness's Celery integration.
#
# One of the generated cases is how the findings were made: it runs send_message through the
# worker once per stage, failing there, then starts the worker again and compares each run with a
# normal one. A worker that loses its pool child after the task's SUCCESS is stored ends with the
# task SUCCESS and its error callback fired as well. The contract declares that as a gap, so it
# is reported as a strict XFAIL; the day every stage converges, it passes, and the strict marker
# fails the run until the gap is removed.
@due_work_contract_suite(CONTRACT)
class test_WorkerContract:
    """Every case in this class is generated from CONTRACT; see the comment above."""
