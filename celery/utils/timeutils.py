# -*- coding: utf-8 -*-
"""
    celery.utils.timeutils
    ~~~~~~~~~~~~~~~~~~~~~~

    This module contains various utilities relating to dates and times.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import math

from datetime import datetime, timedelta
from dateutil.parser import parse as parse_iso8601

DAYNAMES = "sun", "mon", "tue", "wed", "thu", "fri", "sat"
WEEKDAYS = dict((name, dow) for name, dow in zip(DAYNAMES, range(7)))

RATE_MODIFIER_MAP = {"s": lambda n: n,
                     "m": lambda n: n / 60.0,
                     "h": lambda n: n / 60.0 / 60.0}

HAVE_TIMEDELTA_TOTAL_SECONDS = hasattr(timedelta, "total_seconds")

TIME_UNITS = (("day", 60 * 60 * 24, lambda n: int(math.ceil(n))),
              ("hour", 60 * 60, lambda n: int(math.ceil(n))),
              ("minute", 60, lambda n: int(math.ceil(n))),
              ("second", 1, lambda n: "%.2f" % n))


def maybe_timedelta(delta):
    """Coerces integer to timedelta if `delta` is an integer."""
    if isinstance(delta, (int, float)):
        return timedelta(seconds=delta)
    return delta


if HAVE_TIMEDELTA_TOTAL_SECONDS:   # pragma: no cover

    def timedelta_seconds(delta):
        """Convert :class:`datetime.timedelta` to seconds.

        Doesn't account for negative values.

        """
        return max(delta.total_seconds(), 0)
else:  # pragma: no cover

    def timedelta_seconds(delta):  # noqa
        """Convert :class:`datetime.timedelta` to seconds.

        Doesn't account for negative values.

        """
        if delta.days < 0:
            return 0
        return delta.days * 86400 + delta.seconds + (delta.microseconds / 10e5)


def delta_resolution(dt, delta):
    """Round a datetime to the resolution of a timedelta.

    If the timedelta is in days, the datetime will be rounded
    to the nearest days, if the timedelta is in hours the datetime
    will be rounded to the nearest hour, and so on until seconds
    which will just return the original datetime.

    """
    delta = timedelta_seconds(delta)

    resolutions = ((3, lambda x: x / 86400),
                   (4, lambda x: x / 3600),
                   (5, lambda x: x / 60))

    args = dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
    for res, predicate in resolutions:
        if predicate(delta) >= 1.0:
            return datetime(*args[:res])
    return dt


def remaining(start, ends_in, now=None, relative=True):
    """Calculate the remaining time for a start date and a timedelta.

    e.g. "how many seconds left for 30 seconds after start?"

    :param start: Start :class:`~datetime.datetime`.
    :param ends_in: The end delta as a :class:`~datetime.timedelta`.
    :keyword relative: If set to :const:`False`, the end time will be
        calculated using :func:`delta_resolution` (i.e. rounded to the
        resolution of `ends_in`).
    :keyword now: Function returning the current time and date,
        defaults to :func:`datetime.now`.

    """
    now = now or datetime.now()

    end_date = start + ends_in
    if not relative:
        end_date = delta_resolution(end_date, ends_in)
    return end_date - now


def rate(rate):
    """Parses rate strings, such as `"100/m"` or `"2/h"`
    and converts them to seconds."""
    if rate:
        if isinstance(rate, basestring):
            ops, _, modifier = rate.partition("/")
            return RATE_MODIFIER_MAP[modifier or "s"](int(ops)) or 0
        return rate or 0
    return 0


def weekday(name):
    """Return the position of a weekday (0 - 7, where 0 is Sunday).

    Example::

        >>> weekday("sunday"), weekday("sun"), weekday("mon")
        (0, 0, 1)

    """
    abbreviation = name[0:3].lower()
    try:
        return WEEKDAYS[abbreviation]
    except KeyError:
        # Show original day name in exception, instead of abbr.
        raise KeyError(name)


def humanize_seconds(secs, prefix=""):
    """Show seconds in human form, e.g. 60 is "1 minute", 7200 is "2
    hours"."""
    for unit, divider, formatter in TIME_UNITS:
        if secs >= divider:
            w = secs / divider
            punit = w > 1 and (unit + "s") or unit
            return "%s%s %s" % (prefix, formatter(w), punit)
    return "now"


def maybe_iso8601(dt):
    """`Either datetime | str -> datetime or None -> None`"""
    if not dt:
        return
    if isinstance(dt, datetime):
        return dt
    return parse_iso8601(dt)
