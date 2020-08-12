from datetime import timedelta

import pytest

from celery.schedules import schedule
from celery.task import PeriodicTask, periodic_task


class test_periodic_tasks:

    def setup(self):
        self.app.set_current()  # @depends_on_current_app

        @periodic_task(app=self.app, shared=False,
                       run_every=schedule(timedelta(hours=1), app=self.app))
        def my_periodic():
            pass
        self.my_periodic = my_periodic

    def now(self):
        return self.app.now()

    def test_must_have_run_every(self):
        with pytest.raises(NotImplementedError):
            type('Foo', (PeriodicTask,), {
                '__module__': __name__,
            })

    def test_remaining_estimate(self):
        s = self.my_periodic.run_every
        assert isinstance(
            s.remaining_estimate(s.maybe_make_aware(self.now())),
            timedelta)

    def test_is_due_not_due(self):
        due, remaining = self.my_periodic.run_every.is_due(self.now())
        assert not due
        # This assertion may fail if executed in the
        # first minute of an hour, thus 59 instead of 60
        assert remaining > 59

    def test_is_due(self):
        p = self.my_periodic
        due, remaining = p.run_every.is_due(
            self.now() - p.run_every.run_every,
        )
        assert due
        assert remaining == p.run_every.run_every.total_seconds()

    def test_schedule_repr(self):
        p = self.my_periodic
        assert repr(p.run_every)
