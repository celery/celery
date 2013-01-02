# -*- coding: utf-8 -*-
"""
    celery.utils.timeutils
    ~~~~~~~~~~~~~~~~~~~~~~

    This module contains various utilities related to dates and times.

"""
from __future__ import absolute_import

import os
import time as _time

from kombu.utils import cached_property

from datetime import datetime, timedelta, tzinfo
from dateutil import tz
from dateutil.parser import parse as parse_iso8601

from celery.exceptions import ImproperlyConfigured

from .text import pluralize

try:
    import pytz
    from pytz import AmbiguousTimeError
except ImportError:                         # pragma: no cover
    pytz = None                             # noqa

    class AmbiguousTimeError(Exception):    # noqa
        pass


C_REMDEBUG = os.environ.get('C_REMDEBUG', False)


DAYNAMES = 'sun', 'mon', 'tue', 'wed', 'thu', 'fri', 'sat'
WEEKDAYS = dict((name, dow) for name, dow in zip(DAYNAMES, range(7)))

RATE_MODIFIER_MAP = {'s': lambda n: n,
                     'm': lambda n: n / 60.0,
                     'h': lambda n: n / 60.0 / 60.0}


HAVE_TIMEDELTA_TOTAL_SECONDS = hasattr(timedelta, 'total_seconds')

TIME_UNITS = (('day', 60 * 60 * 24.0, lambda n: '%.2f' % n),
              ('hour', 60 * 60.0, lambda n: '%.2f' % n),
              ('minute', 60.0, lambda n: '%.2f' % n),
              ('second', 1.0, lambda n: '%.2f' % n))

ZERO = timedelta(0)

_local_timezone = None


class LocalTimezone(tzinfo):
    """
    Local time implementation taken from Python's docs.

    Used only when pytz isn't available, and most likely inaccurate. If you're
    having trouble with this class, don't waste your time, just install pytz.
    """

    def __init__(self):
        # This code is moved in __init__ to execute it as late as possible
        # See get_default_timezone().
        self.STDOFFSET = timedelta(seconds=-_time.timezone)
        if _time.daylight:
            self.DSTOFFSET = timedelta(seconds=-_time.altzone)
        else:
            self.DSTOFFSET = self.STDOFFSET
        self.DSTDIFF = self.DSTOFFSET - self.STDOFFSET
        tzinfo.__init__(self)

    def __repr__(self):
        return "<LocalTimezone>"

    def utcoffset(self, dt):
        if self._isdst(dt):
            return self.DSTOFFSET
        else:
            return self.STDOFFSET

    def dst(self, dt):
        if self._isdst(dt):
            return self.DSTDIFF
        else:
            return ZERO

    def tzname(self, dt):
        return _time.tzname[self._isdst(dt)]

    def _isdst(self, dt):
        tt = (dt.year, dt.month, dt.day,
              dt.hour, dt.minute, dt.second,
              dt.weekday(), 0, 0)
        stamp = _time.mktime(tt)
        tt = _time.localtime(stamp)
        return tt.tm_isdst > 0


class _Zone(object):

    def tz_or_local(self, tzinfo=None):
        if tzinfo is None:
            return self.local
        return self.get_timezone(tzinfo)

    def to_local(self, dt, local=None, orig=None):
        if is_naive(dt):
            dt = make_aware(dt, orig or self.utc)
        return localize(dt, self.tz_or_local(local))

    def to_system(self, dt):
        return localize(dt, self.local)

    def to_local_fallback(self, dt, *args, **kwargs):
        if is_naive(dt):
            return make_aware(dt, self.local)
        return localize(dt, self.local)

    def get_timezone(self, zone):
        if isinstance(zone, basestring):
            if pytz is None:
                if zone == 'UTC':
                    return tz.gettz('UTC')
                raise ImproperlyConfigured(
                    'Timezones requires the pytz library')
            return pytz.timezone(zone)
        return zone

    @cached_property
    def local(self):
        return LocalTimezone()

    @cached_property
    def utc(self):
        return self.get_timezone('UTC')
timezone = _Zone()


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


def remaining(start, ends_in, now=None, relative=False, debug=False):
    """Calculate the remaining time for a start date and a timedelta.

    e.g. "how many seconds left for 30 seconds after start?"

    :param start: Start :class:`~datetime.datetime`.
    :param ends_in: The end delta as a :class:`~datetime.timedelta`.
    :keyword relative: If enabled the end time will be
        calculated using :func:`delta_resolution` (i.e. rounded to the
        resolution of `ends_in`).
    :keyword now: Function returning the current time and date,
        defaults to :func:`datetime.utcnow`.

    """
    now = now or datetime.utcnow()
    end_date = start + ends_in
    if relative:
        end_date = delta_resolution(end_date, ends_in)
    ret = end_date - now
    if C_REMDEBUG:
        print('rem: NOW:%s START:%s END_DATE:%s REM:%s' % (
            now, start, end_date, ret))
    return ret


def rate(rate):
    """Parses rate strings, such as `"100/m"`, `"2/h"` or `"0.5/s"`
    and converts them to seconds."""
    if rate:
        if isinstance(rate, basestring):
            ops, _, modifier = rate.partition('/')
            return RATE_MODIFIER_MAP[modifier or 's'](float(ops)) or 0
        return rate or 0
    return 0


def weekday(name):
    """Return the position of a weekday (0 - 7, where 0 is Sunday).

    Example::

        >>> weekday('sunday'), weekday('sun'), weekday('mon')
        (0, 0, 1)

    """
    abbreviation = name[0:3].lower()
    try:
        return WEEKDAYS[abbreviation]
    except KeyError:
        # Show original day name in exception, instead of abbr.
        raise KeyError(name)


def humanize_seconds(secs, prefix='', sep=''):
    """Show seconds in human form, e.g. 60 is "1 minute", 7200 is "2
    hours".

    :keyword prefix: Can be used to add a preposition to the output,
        e.g. 'in' will give 'in 1 second', but add nothing to 'now'.

    """
    secs = float(secs)
    for unit, divider, formatter in TIME_UNITS:
        if secs >= divider:
            w = secs / divider
            return '%s%s%s %s' % (prefix, sep, formatter(w),
                                  pluralize(w, unit))
    return 'now'


def maybe_iso8601(dt):
    """`Either datetime | str -> datetime or None -> None`"""
    if not dt:
        return
    if isinstance(dt, datetime):
        return dt
    return parse_iso8601(dt)


def is_naive(dt):
    """Returns :const:`True` if the datetime is naive
    (does not have timezone information)."""
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None


def make_aware(dt, tz):
    """Sets the timezone for a datetime object."""
    try:
        _localize = tz.localize
    except AttributeError:
        return dt.replace(tzinfo=tz)
    else:
        # works on pytz timezones
        try:
            return _localize(dt, is_dst=None)
        except AmbiguousTimeError:
            return min(_localize(dt, is_dst=True),
                       _localize(dt, is_dst=False))


def localize(dt, tz):
    """Convert aware datetime to another timezone."""
    dt = dt.astimezone(tz)
    try:
        _normalize = tz.normalize
    except AttributeError:  # non-pytz tz
        return dt
    else:
        try:
            return _normalize(dt, is_dst=None)
        except TypeError:
            return _normalize(dt)
        except AmbiguousTimeError:
            return min(_normalize(dt, is_dst=True),
                       _normalize(dt, is_dst=False))


def to_utc(dt):
    """Converts naive datetime to UTC"""
    return make_aware(dt, timezone.utc)


def maybe_make_aware(dt, tz=None):
    if is_naive(dt):
        dt = to_utc(dt)
    return localize(dt,
                    timezone.utc if tz is None else timezone.tz_or_local(tz))
