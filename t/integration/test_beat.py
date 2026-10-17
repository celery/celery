from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from time import monotonic, sleep
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from dateutil import tz as dateutil_tz

from celery import beat
from celery.schedules import crontab
from t.integration.tasks import add

from .conftest import flaky


class test_beat_interval_timezones:
    @flaky
    @pytest.mark.usefixtures('celery_session_worker')
    @pytest.mark.parametrize('tz_factory', [
        pytest.param(ZoneInfo, id='zoneinfo'),
        pytest.param(dateutil_tz.gettz, id='dateutil'),
    ])
    @pytest.mark.parametrize(
        'timezone_name,interval,relative,last_run,due_at,next_due_at', [
            pytest.param(
                'America/New_York', timedelta(days=1), True,
                datetime(2026, 11, 1, 0, 30), datetime(2026, 11, 2),
                datetime(2026, 11, 3),
                id='daily-fall-back',
            ),
            pytest.param(
                'America/New_York', timedelta(days=1), True,
                datetime(2026, 3, 7, 23, 30), datetime(2026, 3, 8),
                datetime(2026, 3, 9),
                id='daily-spring-forward',
            ),
            pytest.param(
                'America/New_York', timedelta(days=1), True,
                datetime(2026, 3, 8, 0, 30), datetime(2026, 3, 9),
                datetime(2026, 3, 10),
                id='daily-across-spring-forward',
            ),
            pytest.param(
                'America/New_York', timedelta(hours=1), True,
                datetime(2026, 3, 8, 1, 30), datetime(2026, 3, 8, 3),
                datetime(2026, 3, 8, 4),
                id='hourly-across-spring-forward',
            ),
            pytest.param(
                'Australia/Lord_Howe', timedelta(hours=1), True,
                datetime(2026, 10, 4, 1, 15), datetime(2026, 10, 4, 2, 30),
                datetime(2026, 10, 4, 3),
                id='hourly-half-hour-spring-forward',
            ),
            pytest.param(
                'Africa/Cairo', timedelta(days=1), True,
                datetime(2026, 4, 23, 23, 30), datetime(2026, 4, 24, 1),
                datetime(2026, 4, 25),
                id='daily-cairo-midnight-gap',
            ),
            pytest.param(
                'America/Havana', timedelta(days=1), True,
                datetime(2026, 3, 7, 23, 30), datetime(2026, 3, 8, 1),
                datetime(2026, 3, 9),
                id='daily-havana-midnight-gap',
            ),
            pytest.param(
                'America/Goose_Bay', timedelta(days=1), True,
                datetime(2000, 10, 28, 23, 30, fold=1),
                datetime(2000, 10, 29, fold=1),
                datetime(2000, 10, 30),
                id='daily-repeated-midnight',
            ),
            pytest.param(
                'Pacific/Chatham', timedelta(hours=2), True,
                datetime(2026, 4, 5, 2, 45),
                datetime(2026, 4, 5, 3, fold=1),
                datetime(2026, 4, 5, 5),
                id='two-hours-after-repeated-period',
            ),
            pytest.param(
                'Australia/Lord_Howe', timedelta(hours=1), True,
                datetime(2026, 4, 5, 1), datetime(2026, 4, 5, 2),
                datetime(2026, 4, 5, 3),
                id='hourly-half-hour-fall-back',
            ),
            pytest.param(
                'America/New_York', timedelta(days=1), False,
                datetime(2026, 11, 1, 0, 30), datetime(2026, 11, 1, 23, 30),
                datetime(2026, 11, 2, 23, 30),
                id='elapsed-day-fall-back',
            ),
            pytest.param(
                'America/New_York', timedelta(days=1), False,
                datetime(2026, 3, 7, 23, 30), datetime(2026, 3, 9, 0, 30),
                datetime(2026, 3, 10, 0, 30),
                id='elapsed-day-spring-forward',
            ),
            pytest.param(
                'Africa/Cairo', timedelta(days=1), False,
                datetime(2026, 4, 23, 23, 30), datetime(2026, 4, 25, 0, 30),
                datetime(2026, 4, 26, 0, 30),
                id='elapsed-day-midnight-gap',
            ),
        ],
    )
    def test_dispatches_once_at_deadline(
        self, app, monkeypatch, timezone_name, interval, relative,
        last_run, due_at, next_due_at, tz_factory,
    ):
        zone = tz_factory(timezone_name)
        app.conf.timezone = zone
        last_run = last_run.replace(tzinfo=zone)
        due_utc = due_at.replace(tzinfo=zone).astimezone(timezone.utc)
        now = last_run
        # Control only Beat's clock; the broker and worker use real time.
        monkeypatch.setattr(app, 'now', lambda: now)
        task_id = uuid4().hex
        scheduler = beat.Scheduler(app=app, lazy=True)
        entry = scheduler.add(
            name='timezone-interval', task=add.name, args=(1, 2),
            schedule=interval, relative=relative, last_run_at=last_run,
            options={'task_id': task_id},
        )

        assert scheduler.tick() > 0
        assert scheduler.schedule[entry.name].total_run_count == 0

        # Move through the transition without waiting for wall-clock time.
        now = (due_utc - timedelta(seconds=1)).astimezone(zone)
        assert scheduler.tick() > 0
        assert scheduler.schedule[entry.name].total_run_count == 0

        now = due_utc.astimezone(zone)
        assert scheduler.tick() == 0
        assert app.AsyncResult(task_id).get(timeout=30) == 3
        assert scheduler.schedule[entry.name].total_run_count == 1
        last_dispatch = scheduler.schedule[entry.name].last_run_at
        assert last_dispatch.astimezone(timezone.utc) == due_utc

        # Further ticks must not dispatch the same deadline again.
        assert scheduler.tick() > 0
        now = (due_utc + timedelta(seconds=1)).astimezone(zone)
        assert scheduler.tick() > 0
        assert scheduler.schedule[entry.name].total_run_count == 1

        # Keep the populated heap and verify the following run as well.
        next_due_utc = next_due_at.replace(tzinfo=zone).astimezone(timezone.utc)
        next_task_id = uuid4().hex
        scheduler.schedule[entry.name].options['task_id'] = next_task_id
        now = (next_due_utc - timedelta(seconds=1)).astimezone(zone)
        assert scheduler.tick() > 0
        assert scheduler.schedule[entry.name].total_run_count == 1

        now = next_due_utc.astimezone(zone)
        assert scheduler.tick() == 0
        assert app.AsyncResult(next_task_id).get(timeout=30) == 3
        assert scheduler.schedule[entry.name].total_run_count == 2
        last_dispatch = scheduler.schedule[entry.name].last_run_at
        assert last_dispatch.astimezone(timezone.utc) == next_due_utc
        assert scheduler.tick() > 0
        assert scheduler.schedule[entry.name].total_run_count == 2


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


class test_beat_remote_control:
    """A running beat must answer while it ticks, and stop when it wedges.

    The unit and smoke tests both write ``Service._last_tick`` directly,
    so neither covers the scheduler loop keeping the node answerable on
    its own.  That gap matters: a false positive there silences a
    healthy beat, and on a liveness probe it restarts one that is
    working fine.
    """

    #: kept short so the test doesn't have to wait out a real window;
    #: an explicit setting also bypasses BeatPidbox.min_tick_age.
    max_tick_age = 2.0

    @staticmethod
    def _wait_until(predicate, timeout):
        deadline = monotonic() + timeout
        while monotonic() < deadline:
            if predicate():
                return True
            sleep(0.2)
        return False

    @pytest.fixture
    def beat_node(self, app):
        app.conf.beat_enable_remote_control = True
        app.conf.beat_remote_control_max_tick_age = self.max_tick_age
        app.conf.beat_schedule = {}
        service = beat.Service(
            app=app,
            max_interval=0.5,
            scheduler_cls='celery.beat:Scheduler',
            hostname=f'celerybeat-{uuid4().hex[:8]}@%h',
        )
        thread = Thread(target=service.start, name='beat-integration',
                        daemon=True)
        thread.start()
        node = beat.beat_nodename(service.hostname)
        try:
            if not self._wait_until(
                lambda: app.control.ping(destination=[node], timeout=3),
                timeout=25,
            ):
                # Say which of the two it was: a node that never came up
                # looks identical to one that came up and went stale
                # immediately because the loop stopped refreshing.
                pidbox = service._pidbox
                pytest.fail(
                    f'beat never answered a ping: thread_alive='
                    f'{thread.is_alive()} pidbox='
                    f'{pidbox is not None and pidbox.thread is not None} '
                    f'tick_age={pidbox and pidbox.tick_age()}')
            yield service, node
        finally:
            service.stop()
            thread.join(timeout=30)

    @flaky
    def test_a_ticking_beat_keeps_answering(self, app, beat_node):
        _, node = beat_node
        # Well past the staleness window: a loop that keeps up must never
        # let the node fall silent.
        deadline = monotonic() + self.max_tick_age * 4
        while monotonic() < deadline:
            assert app.control.ping(destination=[node], timeout=5) == [
                {node: {'ok': 'pong'}}
            ], 'a healthy beat stopped answering'
            sleep(0.25)

    @flaky
    def test_a_wedged_scheduler_falls_silent(self, app, beat_node):
        service, node = beat_node
        # Wedge the loop the way a stuck tick would, rather than writing
        # _last_tick: the pidbox thread stays up and connected, and only
        # the scheduler stops advancing.
        released = Event()

        def wedged_tick(*args, **kwargs):
            released.wait()
            return 0.5

        service.scheduler.tick = wedged_tick
        try:
            assert self._wait_until(
                lambda: app.control.ping(destination=[node],
                                         timeout=3) == [],
                timeout=20,
            ), 'a wedged beat kept answering'
        finally:
            # let the loop run again so the fixture can stop it
            released.set()
