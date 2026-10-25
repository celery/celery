"""
Celery's contract with its worker, checked with pytest-obligation.

pytest-obligation (https://github.com/gigaverse-app/pytest-obligation) is a
pytest plugin that checks background work is neither lost nor run twice. A
contract names the guarantees a system offers and binds each to real code; the
harness then generates the test cases.

This contract binds Celery's worker itself, through ``worker_contract`` from
the harness's Celery worker integration. Two histories go through a real
``celery worker`` with one prefork child, run as a child process of the test:

* one task, ``send_message``, with a link (``announce``) and an error callback
  (``report_failure``);
* a chord of two ``send_message`` tasks whose body (``announce_all``) has the
  same error callback.

The harness fails each at Celery's own stages, on the first execution of
``send_message``:

* the pool child dies at ``task_prerun``, at ``mark_as_done`` (the body ran and
  the link was published, the result not stored yet), and at ``task_postrun``
  (the result stored as SUCCESS, the parent not told);
* the task's ``on_success`` hook raises, and the broker refuses the first
  signature the task publishes: the link, or the chord's body.

Recovery is the worker starting again. Each history must end where normal
operation does. What diverges is declared as a gap per history, a strict xfail,
and its ``Findings`` pin what every failure leaves, in the same run as the
verdict.

This directory is not part of the integration suite, which shares one session
worker: its tests start their own workers. Run it (Python 3.11 or later, Linux
or macOS, and a Redis on localhost)::

    pip install -e . -r requirements/test.txt -r requirements/test-due-work.txt
    pytest t/due_work -p no:cacheprovider
"""

import redis
from pytest_obligation import Findings, due_work_contract_suite
from pytest_obligation.integrations.celery_worker import worker_contract, worker_history

from celery import chord
from celery.result import AsyncResult

from . import app as due_work

MESSAGE = 'your order has shipped'
SENT = f'sent {MESSAGE!r}'


def empty():
    # ARRANGE: an empty broker, result backend and record.
    for url in (due_work.BROKER, due_work.BACKEND):
        redis.Redis.from_url(url).flushdb()
    due_work.RECORDS.flushdb()


def send():
    # ARRANGE: the task, with its link and error callback.
    empty()
    return due_work.send_message.apply_async(
        (MESSAGE,), link=due_work.announce.s(), link_error=due_work.report_failure.s()
    ).id


def send_chord():
    # ARRANGE: two messages, and a body that announces them once both are sent.
    empty()
    header = [due_work.send_message.s(f'order {n} has shipped') for n in (1, 2)]
    return chord(header)(due_work.announce_all.s().on_error(due_work.report_failure.s())).id


def what_happened(task_id):
    # OBSERVE: the task's recorded state, how many messages left, and what the callbacks announced.
    return (
        AsyncResult(task_id, app=due_work.app).state,
        due_work.RECORDS.llen('sent'),
        # Sorted: the link and the error callback run in different processes, in either order.
        tuple(sorted(entry.decode() for entry in due_work.RECORDS.lrange('announced', 0, -1))),
    )


def settled(task_id):
    return AsyncResult(task_id, app=due_work.app).ready()


# What each failure leaves after the worker starts again, pinned in the same run as the
# verdict; every history not listed reaches normal operation. A change in the worker moves
# an entry, and the case names it. Celery's main fixed two earlier entries: a pool child
# lost after SUCCESS was stored (#10752) and an on_success hook that raises (#10759) now
# reach normal operation.
FINDINGS = Findings(
    ('SUCCESS', 1, (SENT,)),
    {
        # The child died as the task started: the message is never sent, the error callback fires, and
        # nothing runs the task again (task_reject_on_worker_lost is off by default).
        'died at task_prerun': ('FAILURE', 0, ('failed: WorkerLostError',)),
        # FINDING: the message was sent and the link announced it; the task is recorded FAILURE and the
        # error callback announces a failure too.
        'died at mark_as_done': ('FAILURE', 1, ('failed: WorkerLostError', SENT)),
        # FINDING (#10725): the message was sent; the refused link records FAILURE, and the message is
        # acknowledged, so the link is never published again.
        "the broker refused the task's link": ('FAILURE', 1, ('failed: OperationalError',)),
    },
)

# The chord's state is its body's. A header task lost after SUCCESS was stored, or whose on_success
# raised, is counted once and the body runs: both fixes hold inside a chord.
CHORD_FINDINGS = Findings(
    ('SUCCESS', 2, ('sent 2 messages',)),
    {
        # The first header's child died as it started: its message is never sent, and the chord fails.
        'died at task_prerun': ('FAILURE', 1, ('failed: ChordError',)),
        # FINDING: both messages were sent; the first header is recorded FAILURE, so the chord fails and its
        # body never runs.
        'died at mark_as_done': ('FAILURE', 2, ('failed: ChordError',)),
        # FINDING (reported in a comment on #10725): both messages were sent; the last header's publish of the
        # body is refused, the chord is recorded FAILURE, and nothing publishes the body again.
        "the broker refused the task's link": ('FAILURE', 2, ('failed: ChordError',)),
    },
)


SEND_MESSAGE = worker_history(
    name='a worker runs send_message',
    app='t.due_work.app:app',
    task=due_work.send_message.name,
    send=send,
    observe=what_happened,
    initial=('PENDING', 0, ()),
    expected=FINDINGS.delivered,
    settled=settled,
    findings=FINDINGS,
)

CHORD = worker_history(
    name='a worker runs a chord of two send_message tasks',
    app='t.due_work.app:app',
    task=due_work.send_message.name,
    send=send_chord,
    observe=what_happened,
    initial=('PENDING', 0, ()),
    expected=CHORD_FINDINGS.delivered,
    settled=settled,
    findings=CHORD_FINDINGS,
)

CONTRACT = worker_contract(
    name='celery: the worker running a task',
    history=SEND_MESSAGE,
    gap=(
        'once the link is published, a failure still runs the failure path: a child lost after the link, before '
        'the store, records FAILURE for a task whose effect and link happened, and fires the error callback '
        'beside the link (not reported separately); and a link the broker refuses records FAILURE and '
        'acknowledges the message, so the link is never sent (https://github.com/celery/celery/issues/10725). '
        'FINDINGS pins each history'
    ),
)
# worker_contract binds one history; the chord is a second process handoff of the same contract.
CONTRACT = CONTRACT.model_copy(
    update={
        'process_handoffs': (*CONTRACT.process_handoffs, CHORD),
        'handoff_gaps': {
            **CONTRACT.handoff_gaps,
            CHORD.name: (
                'a chord whose body the broker refuses, or whose header task is lost after its effect and before '
                'its store, fails for good with ChordError though every message was sent; the body never runs '
                '(https://github.com/celery/celery/issues/10725#issuecomment-6318108091). '
                'CHORD_FINDINGS pins each history'
            ),
        },
    }
)


# This is where the magic happens. The class is empty on purpose: the decorator reads CONTRACT
# and generates its tests, bound to Celery's real worker. No test case is written by hand; this
# file supplies only the tasks and how to see what they did, and the stages where the worker can
# fail come from the harness's Celery integration.
#
# Each history's generated case is how the findings were made: it runs the work through the
# worker once per stage, failing there, then starts the worker again and compares each run with a
# normal one. A task whose link was published, then refused or followed by a lost pool child, ends
# FAILURE with its error callback fired. The contract declares that as a gap, so it is reported as a
# strict XFAIL; the day every stage converges, it passes, and the strict marker fails the run until
# the gap is removed.
@due_work_contract_suite(CONTRACT)
class test_WorkerContract:
    """Every case in this class is generated from CONTRACT; see the comment above."""
