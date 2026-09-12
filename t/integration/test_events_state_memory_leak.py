"""
Integration test for memory leak issue #4832.

Reproduces the scenario of a long-running monitor (e.g. ``celery events``
or Flower) consuming real events off the broker: child tasks whose
parent is never observed (because it was scheduled with an ETA/countdown
and its own event hasn't arrived yet, or because it was already evicted
from ``State.tasks``) used to accumulate forever in
``State._tasks_to_resolve``, since that mapping was a plain, unbounded
``dict``.

Unlike the unit test in ``t/unit/events/test_state.py`` (which calls
``State.event()`` directly with synthetic dicts), this test exercises the
real event pipeline: an ``EventDispatcher`` publishes events onto an
actual (in-memory) broker connection, and an ``EventReceiver`` consumes
and decodes them before handing them to ``State``.
"""

from celery import Celery, uuid


def test_tasks_to_resolve_is_bounded_over_real_event_pipeline():
    app = Celery('test_events_state_memory_leak')
    app.conf.update(broker_url='memory://', result_backend='cache+memory://')

    max_tasks_in_memory = 10
    state = app.events.State(max_tasks_in_memory=max_tasks_in_memory)

    orphan_count = 100

    with app.connection_for_write() as connection:
        receiver = app.events.Receiver(
            connection, handlers={'*': state.event},
        )
        # Declare (and bind) the receiver's queue up front, since the
        # in-memory transport drops messages published before a matching
        # queue exists.
        receiver.queue(connection.default_channel).declare()

        with app.events.Dispatcher(connection) as dispatcher:
            for _ in range(orphan_count):
                # Simulate a child task event arriving before (or without
                # ever seeing) its parent -- e.g. a task scheduled with a
                # countdown/ETA whose parent id is never registered in
                # State.tasks. Each one registers a pending entry in
                # State._tasks_to_resolve keyed by a parent_id that will
                # never show up.
                dispatcher.send(
                    'task-received',
                    uuid=uuid(),
                    parent_id=uuid(),
                    root_id=uuid(),
                    name='task1',
                    args='()',
                    kwargs='{}',
                    retries=0,
                    eta=None,
                )

        receiver.capture(limit=orphan_count, timeout=10)

    assert state.event_count == orphan_count
    assert len(state._tasks_to_resolve) <= max_tasks_in_memory
