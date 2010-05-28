from datetime import datetime

from celery.utils.timeutils import timedelta_seconds, weekday, remaining


class schedule(object):
    relative = False

    def __init__(self, run_every=None, relative=False):
        self.run_every = run_every
        self.relative = relative

    def remaining_estimate(self, last_run_at):
        """Returns when the periodic task should run next as a timedelta."""
        return remaining(last_run_at, self.run_every, relative=self.relative)

    def is_due(self, last_run_at):
        """Returns tuple of two items ``(is_due, next_time_to_run)``,
        where next time to run is in seconds.

        See :meth:`celery.task.base.PeriodicTask.is_due` for more information.

        """
        rem_delta = self.remaining_estimate(last_run_at)
        rem = timedelta_seconds(rem_delta)
        if rem == 0:
            return True, timedelta_seconds(self.run_every)
        return False, rem


class crontab(schedule):
    """A crontab can be used as the ``run_every`` value of a
    :class:`PeriodicTask` to add cron-like scheduling.

    Like a :manpage:`cron` job, you can specify units of time of when
    you would like the task to execute. While not a full implementation
    of cron's features, it should provide a fair degree of common scheduling
    needs.

    You can specify a minute, an hour, and/or a day of the week.

    .. attribute:: minute

        An integer from 0-59 that represents the minute of an hour of when
        execution should occur.

    .. attribute:: hour

        An integer from 0-23 that represents the hour of a day of when
        execution should occur.

    .. attribute:: day_of_week

        An integer from 0-6, where Sunday = 0 and Saturday = 6, that
        represents the day of week that execution should occur.

    """

    def __init__(self, minute=None, hour=None, day_of_week=None,
            nowfun=datetime.now):
        self.hour = hour                  # (0 - 23)
        self.minute = minute              # (0 - 59)
        self.day_of_week = day_of_week    # (0 - 6) (Sunday=0)
        self.nowfun = nowfun

        if isinstance(self.day_of_week, basestring):
            self.day_of_week = weekday(self.day_of_week)

    def remaining_estimate(self, last_run_at):
        # remaining_estimate controls the frequency of scheduler
        # ticks. The scheduler needs to wake up every second in this case.
        return 1

    def is_due(self, last_run_at):
        now = self.nowfun()
        last = now - last_run_at
        due, when = False, 1
        if last.days > 0 or last.seconds > 60:
            if self.day_of_week in (None, now.isoweekday()):
                due, when = self._check_hour_minute(now)
        return due, when

    def _check_hour_minute(self, now):
        due, when = False, 1
        if self.hour is None and self.minute is None:
            due, when = True, 1
        if self.hour is None and self.minute == now.minute:
            due, when = True, 1
        if self.hour == now.hour and self.minute is None:
            due, when = True, 1
        if self.hour == now.hour and self.minute == now.minute:
            due, when = True, 1
        return due, when
