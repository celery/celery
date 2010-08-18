from datetime import datetime
from pyparsing import (Word, Literal, ZeroOrMore, Optional,
                       Group, StringEnd, alphas)

from celery.utils import is_iterable
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


class crontab_parser(object):
    """Parser for crontab expressions. Any expression of the form 'groups' (see
    BNF grammar below) is accepted and expanded to a set of numbers.  These
    numbers represent the units of time that the crontab needs to run on::

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

        minutes = crontab_parser(60).parse("*/15")  # yields [0,15,30,45]
        hours = crontab_parser(24).parse("*/4")     # yields [0,4,8,12,16,20]
        day_of_week = crontab_parser(7).parse("*")  # yields [0,1,2,3,4,5,6]

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
        return filter(lambda x: x != ',', toks)

    @staticmethod
    def _join_to_set(toks):
        return set(toks.asList())

    def parse(self, cronspec):
        return self.parser.parseString(cronspec).pop()


class crontab(schedule):
    """A crontab can be used as the ``run_every`` value of a
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
                        "range is 0-%d. '%d' was found." % (max_, number))

        return result

    def __init__(self, minute='*', hour='*', day_of_week='*',
            nowfun=datetime.now):
        self.hour = self._expand_cronspec(hour, 24)
        self.minute = self._expand_cronspec(minute, 60)
        self.day_of_week = self._expand_cronspec(day_of_week, 7)
        self.nowfun = nowfun

    def __reduce__(self):
        return (self.__class__, (self.minute,
                                 self.hour,
                                 self.day_of_week), None)

    def remaining_estimate(self, last_run_at):
        # remaining_estimate controls the frequency of scheduler
        # ticks. The scheduler needs to wake up every second in this case.
        return 1

    def is_due(self, last_run_at):
        now = self.nowfun()
        last = now - last_run_at
        due, when = False, 1
        if last.days > 0 or last.seconds > 60:
            due = (now.isoweekday() % 7 in self.day_of_week and
                   now.hour in self.hour and
                   now.minute in self.minute)
        return due, when
