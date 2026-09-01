from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from celery import beat
from celery.schedules import crontab
from t.integration.tasks import add

from .conftest import flaky


class test_beat_cron_starting_deadline:
    @flaky
    @pytest.mark.usefixtures('celery_session_worker')
    @pytest.mark.celery(beat_cron_starting_deadline=1800)
    def test_dispatches_missed_cron_within_deadline_non_uniform(self, app):
        # Non-uniform crontab (:00, :45): feasible runs were 9:00, 9:45, 10:00.
        # The most recent (10:00) is 20 min before now=10:20, within the
        # 30-min deadline, so the missed task should dispatch.
        now = datetime(2022, 12, 5, 10, 20)
        last_run = datetime(2022, 12, 5, 8, 45)
        task_id = uuid4().hex

        cron = crontab(minute='0,45', nowfun=lambda: now, app=app)
        scheduler = beat.Scheduler(app=app, lazy=True)

        scheduler.add(
            name='test_beat_deadline_non_uniform',
            task=add.name,
            args=(1, 2),
            schedule=cron,
            last_run_at=last_run,
            options={'task_id': task_id},
        )

        # tick() returns 0 only when it dispatches a due task.
        assert scheduler.tick() == 0
        # The worker received and executed the dispatched task.
        assert app.AsyncResult(task_id).get(timeout=30) == 3


class test_beat_tick_heap_top_changed:
    @flaky
    @pytest.mark.usefixtures('celery_session_worker')
    def test_dispatches_second_entry_after_heap_top_changed(self, app):
        second_task_id = uuid4().hex
        last_run = datetime(2022, 12, 5, 10, 20)
        scheduler = beat.Scheduler(app=app, lazy=True)
        first = scheduler.add(
            name='first',
            task=add.name,
            args=(1, 2),
            schedule=timedelta(seconds=1),
            last_run_at=last_run,
        )
        # so populate_heap() doesn't run and override our setup
        scheduler.old_schedulers = scheduler.schedule
        scheduler._heap = [beat.event_t(scheduler._when(first, 0) - 1, 5, first)]

        def mutating_first_entry_is_due(_last_run_at):
            second = scheduler.add(
                name='second',
                task=add.name,
                args=(3, 4),
                schedule=timedelta(seconds=1),
                last_run_at=last_run,
                options={'task_id': second_task_id},
            )
            scheduler._heap.insert(0, beat.event_t(scheduler._when(second, 0) - 2, 5, second))
            return True, 1

        # simulates an entry inserted while first's is_due() is running
        real_is_due = first.schedule.is_due
        first.schedule.is_due = mutating_first_entry_is_due
        # first is due, but second took the top of the heap while its is_due()
        # ran, so tick() reschedules instead of dispatching it
        assert scheduler.tick() < 0
        assert scheduler.schedule['first'].total_run_count == 0

        first.schedule.is_due = real_is_due
        # tick() returns 0 only when it dispatches a due task, and second is
        # now the top
        assert scheduler.tick() == 0
        assert app.AsyncResult(second_task_id).get(timeout=30) == 7
        assert scheduler.schedule['first'].total_run_count == 0

    @flaky
    @pytest.mark.usefixtures('celery_session_worker')
    def test_dispatches_second_entry_when_first_asks_to_retry_later(self, app):
        second_task_id = uuid4().hex
        last_run = datetime(2022, 12, 5, 10, 20)
        scheduler = beat.Scheduler(app=app, lazy=True)
        first = scheduler.add(
            name='first',
            task=add.name,
            args=(1, 2),
            schedule=timedelta(seconds=1),
            last_run_at=last_run,
        )
        # so populate_heap() doesn't run and override our setup
        scheduler.old_schedulers = scheduler.schedule
        scheduler._heap = [beat.event_t(scheduler._when(first, 0) - 1, 5, first)]

        def mutating_first_entry_is_due(_last_run_at):
            second = scheduler.add(
                name='second',
                task=add.name,
                args=(3, 4),
                schedule=timedelta(seconds=1),
                last_run_at=last_run,
                options={'task_id': second_task_id},
            )
            scheduler._heap.insert(0, beat.event_t(scheduler._when(second, 0) - 2, 5, second))
            # first is ready by heap time, but asks to run a second later
            return False, 1

        # simulates an entry inserted while first's is_due() is running
        real_is_due = first.schedule.is_due
        first.schedule.is_due = mutating_first_entry_is_due
        # The heap says first is ready, but first asks to retry later, and
        # second took the top of the heap while its is_due() ran, so tick()
        # leaves the reheap to the next call
        assert scheduler.tick() < 0

        first.schedule.is_due = real_is_due
        # tick() returns 0 only when it dispatches a due task, and second is
        # now the top
        assert scheduler.tick() == 0
        assert app.AsyncResult(second_task_id).get(timeout=30) == 7
        assert scheduler.schedule['first'].total_run_count == 0
