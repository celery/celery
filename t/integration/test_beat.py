from datetime import datetime
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
