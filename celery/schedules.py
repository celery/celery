from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyparsing import (Word, Literal, ZeroOrMore, Optional,
                       Group, StringEnd, alphas)

from celery.utils import is_iterable
from celery.utils.timeutils import (timedelta_seconds, weekday,
                                    remaining, humanize_seconds)


class schedule(object):
    relative = False

    def __init__(self, run_every=None, relative=False):
        self.run_every = run_every
        self.relative = relative

    def remaining_estimate(self, last_run_at):
        """Returns when the periodic task should run next as a timedelta."""
        return remaining(last_run_at, self.run_every, relative=self.relative)

    def is_due(self, last_run_at):
        """Returns tuple of two items `(is_due, next_time_to_run)`,
        where next time to run is in seconds.

        e.g.

        * `(True, 20)`, means the task should be run now, and the next
            time to run is in 20 seconds.

        * `(False, 12)`, means the task should be run in 12 seconds.

        You can override this to decide the interval at runtime,
        but keep in mind the value of :setting:`CELERYBEAT_MAX_LOOP_INTERVAL`,
        which decides the maximum number of seconds celerybeat can sleep
        between re-checking the periodic task intervals.  So if you
        dynamically change the next run at value, and the max interval is
        set to 5 minutes, it will take 5 minutes for the change to take
        effect, so you may consider lowering the value of
        :setting:`CELERYBEAT_MAX_LOOP_INTERVAL` if responsiveness is of
        importance to you.

        """
        rem_delta = self.remaining_estimate(last_run_at)
        rem = timedelta_seconds(rem_delta)
        if rem == 0:
            return True, timedelta_seconds(self.run_every)
        return False, rem

    def __repr__(self):
        return "<freq: %s>" % humanize_seconds(
                timedelta_seconds(self.run_every))

    def __eq__(self, other):
        if isinstance(other, schedule):
            return self.run_every == other.run_every
        return self.run_every == other


class crontab_parser(object):
    """Parser for crontab expressions. Any expression of the form 'groups'
    (see BNF grammar below) is accepted and expanded to a set of numbers.
    These numbers represent the units of time that the crontab needs to
    run on::

        digit   :: '0'..'9'
        dow     :: 'a'..'z'
        number  :: digit+ | dow+
        steps   :: number
        range   :: number ( '-' number ) ?
        numspec :: '*' | range
        expr    :: numspec ( '/' steps ) ?
        groups  :: expr ( ',' expr ) *

    The parser is a general purpose one, useful for parsing hours, minutes and
    day_of_week expressions.  Example usage::

        >>> minutes = crontab_parser(60).parse("*/15")
        [0, 15, 30, 45]
        >>> hours = crontab_parser(24).parse("*/4")
        [0, 4, 8, 12, 16, 20]
        >>> day_of_week = crontab_parser(7).parse("*")
        [0, 1, 2, 3, 4, 5, 6]

    """

    def __init__(self, max_=60):
        # define the grammar structure
        digits = "0123456789"
        star = Literal('*')
        number = Word(digits) | Word(alphas)
        steps = number
        range_ = number + Optional(Literal('-') + number)
        numspec = star | range_
        expr = Group(numspec) + Optional(Literal('/') + steps)
        extra_groups = ZeroOrMore(Literal(',') + expr)
        groups = expr + extra_groups + StringEnd()

        # define parse actions
        star.setParseAction(self._expand_star)
        number.setParseAction(self._expand_number)
        range_.setParseAction(self._expand_range)
        expr.setParseAction(self._filter_steps)
        extra_groups.setParseAction(self._ignore_comma)
        groups.setParseAction(self._join_to_set)

        self.max_ = max_
        self.parser = groups

    @staticmethod
    def _expand_number(toks):
        try:
            i = int(toks[0])
        except ValueError:
            try:
                i = weekday(toks[0])
            except KeyError:
                raise ValueError("Invalid weekday literal '%s'." % toks[0])
        return [i]

    @staticmethod
    def _expand_range(toks):
        if len(toks) > 1:
            return range(toks[0], int(toks[2]) + 1)
        else:
            return toks[0]

    def _expand_star(self, toks):
        return range(self.max_)

    @staticmethod
    def _filter_steps(toks):
        numbers = toks[0]
        if len(toks) > 1:
            steps = toks[2]
            return [n for n in numbers if n % steps == 0]
        else:
            return numbers

    @staticmethod
    def _ignore_comma(toks):
        return [x for x in toks if x != ',']

    @staticmethod
    def _join_to_set(toks):
        return set(toks.asList())

    def parse(self, cronspec):
        return self.parser.parseString(cronspec).pop()


class crontab(schedule):
    """A crontab can be used as the `run_every` value of a
    :class:`PeriodicTask` to add cron-like scheduling.

    Like a :manpage:`cron` job, you can specify units of time of when
    you would like the task to execute. It is a reasonably complete
    implementation of cron's features, so it should provide a fair
    degree of scheduling needs.

    You can specify a minute, an hour, and/or a day of the week in any
    of the following formats:

    .. attribute:: minute

        - A (list of) integers from 0-59 that represent the minutes of
          an hour of when execution should occur; or
        - A string representing a crontab pattern.  This may get pretty
          advanced, like `minute="*/15"` (for every quarter) or
          `minute="1,13,30-45,50-59/2"`.

    .. attribute:: hour

        - A (list of) integers from 0-23 that represent the hours of
          a day of when execution should occur; or
        - A string representing a crontab pattern.  This may get pretty
          advanced, like `hour="*/3"` (for every three hours) or
          `hour="0,8-17/2"` (at midnight, and every two hours during
          office hours).

    .. attribute:: day_of_week

        - A (list of) integers from 0-6, where Sunday = 0 and Saturday =
          6, that represent the days of a week that execution should
          occur.
        - A string representing a crontab pattern.  This may get pretty
          advanced, like `day_of_week="mon-fri"` (for weekdays only).
          (Beware that `day_of_week="*/2"` does not literally mean
          "every two days", but "every day that is divisible by two"!)

    """

    @staticmethod
    def _expand_cronspec(cronspec, max_):
        """Takes the given cronspec argument in one of the forms::

            int         (like 7)
            basestring  (like '3-5,*/15', '*', or 'monday')
            set         (like set([0,15,30,45]))
            list        (like [8-17])

        And convert it to an (expanded) set representing all time unit
        values on which the crontab triggers.  Only in case of the base
        type being 'basestring', parsing occurs.  (It is fast and
        happens only once for each crontab instance, so there is no
        significant performance overhead involved.)

        For the other base types, merely Python type conversions happen.

        The argument `max_` is needed to determine the expansion of '*'.

        """
        if isinstance(cronspec, int):
            result = set([cronspec])
        elif isinstance(cronspec, basestring):
            result = crontab_parser(max_).parse(cronspec)
        elif isinstance(cronspec, set):
            result = cronspec
        elif is_iterable(cronspec):
            result = set(cronspec)
        else:
            raise TypeError(
                    "Argument cronspec needs to be of any of the "
                    "following types: int, basestring, or an iterable type. "
                    "'%s' was given." % type(cronspec))

        # assure the result does not exceed the max
        for number in result:
            if number >= max_:
                raise ValueError(
                        "Invalid crontab pattern. Valid "
                        "range is 0-%d. '%d' was found." % (max_ - 1, number))

        return result

    def __init__(self, minute='*', hour='*', day_of_week='*',
            nowfun=datetime.now):
        self._orig_minute = minute
        self._orig_hour = hour
        self._orig_day_of_week = day_of_week
        self.hour = self._expand_cronspec(hour, 24)
        self.minute = self._expand_cronspec(minute, 60)
        self.day_of_week = self._expand_cronspec(day_of_week, 7)
        self.nowfun = nowfun

    def __repr__(self):
        return "<crontab: %s %s %s (m/h/d)>" % (self._orig_minute or "*",
                                                self._orig_hour or "*",
                                                self._orig_day_of_week or "*")

    def __reduce__(self):
        return (self.__class__, (self._orig_minute,
                                 self._orig_hour,
                                 self._orig_day_of_week), None)

    def remaining_estimate(self, last_run_at):
        """Returns when the periodic task should run next as a timedelta."""
        weekday = last_run_at.isoweekday()
        if weekday == 7:    # Sunday is day 0, not day 7.
            weekday = 0

        execute_this_hour = (weekday in self.day_of_week and
                                last_run_at.hour in self.hour and
                                last_run_at.minute < max(self.minute))

        if execute_this_hour:
            next_minute = min(minute for minute in self.minute
                                        if minute > last_run_at.minute)
            delta = relativedelta(minute=next_minute,
                                  second=0,
                                  microsecond=0)
        else:
            next_minute = min(self.minute)

            execute_today = (weekday in self.day_of_week and
                                 last_run_at.hour < max(self.hour))

            if execute_today:
                next_hour = min(hour for hour in self.hour
                                        if hour > last_run_at.hour)
                delta = relativedelta(hour=next_hour,
                                      minute=next_minute,
                                      second=0,
                                      microsecond=0)
            else:
                next_hour = min(self.hour)
                next_day = min([day for day in self.day_of_week
                                    if day > weekday] or
                               self.day_of_week)
                add_week = next_day == weekday

                delta = relativedelta(weeks=add_week and 1 or 0,
                                      weekday=(next_day - 1) % 7,
                                      hour=next_hour,
                                      minute=next_minute,
                                      second=0,
                                      microsecond=0)

        return remaining(last_run_at, delta, now=self.nowfun())

    def is_due(self, last_run_at):
        """Returns tuple of two items `(is_due, next_time_to_run)`,
        where next time to run is in seconds.

        See :meth:`celery.schedules.schedule.is_due` for more information.

        """
        rem_delta = self.remaining_estimate(last_run_at)
        rem = timedelta_seconds(rem_delta)
        due = rem == 0
        if due:
            rem_delta = self.remaining_estimate(last_run_at=self.nowfun())
            rem = timedelta_seconds(rem_delta)
        return due, rem

    def __eq__(self, other):
        if isinstance(other, crontab):
            return (other.day_of_week == self.day_of_week and
                    other.hour == self.hour and
                    other.minute == self.minute)
        return other is self


def maybe_schedule(s, relative=False):
    if isinstance(s, int):
        s = timedelta(seconds=s)
    if isinstance(s, timedelta):
        return schedule(s, relative)
    return s
