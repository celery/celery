from datetime import datetime

from carrot.utils import partition

DAYNAMES = "sun", "mon", "tue", "wed", "thu", "fri", "sat"
WEEKDAYS = dict((name, dow) for name, dow in zip(DAYNAMES, range(7)))

RATE_MODIFIER_MAP = {"s": lambda n: n,
                     "m": lambda n: n / 60.0,
                     "h": lambda n: n / 60.0 / 60.0}


def timedelta_seconds(delta):
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

        >>> now = datetime.now()
        >>> now
        datetime.datetime(2010, 3, 30, 11, 50, 58, 41065)
        >>> delta_resolution(now, timedelta(days=2))
        datetime.datetime(2010, 3, 30, 0, 0)
        >>> delta_resolution(now, timedelta(hours=2))
        datetime.datetime(2010, 3, 30, 11, 0)
        >>> delta_resolution(now, timedelta(minutes=2))
        datetime.datetime(2010, 3, 30, 11, 50)
        >>> delta_resolution(now, timedelta(seconds=2))
        datetime.datetime(2010, 3, 30, 11, 50, 58, 41065)

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


def rate(rate):
    """Parses rate strings, such as ``"100/m"`` or ``"2/h"``
    and converts them to seconds."""
    if rate:
        if isinstance(rate, basestring):
            ops, _, modifier = partition(rate, "/")
            return RATE_MODIFIER_MAP[modifier or "s"](int(ops)) or 0
        return rate or 0
    return 0


def weekday(name):
    """Return the position of a weekday (0 - 7, where 0 is Sunday).

        >>> weekday("sunday")
        0
        >>> weekday("sun")
        0
        >>> weekday("mon")
        1

    """
    abbreviation = name[0:3].lower()
    try:
        return WEEKDAYS[abbreviation]
    except KeyError:
        # Show original day name in exception, instead of abbr.
        raise KeyError(name)
