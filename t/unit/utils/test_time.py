import sys
from datetime import datetime, timedelta
from datetime import timezone as _timezone
from datetime import tzinfo
from unittest.mock import Mock, patch

import pytest

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo

from celery.utils.iso8601 import parse_iso8601
from celery.utils.time import (LocalTimezone, delta_resolution, ffwd, get_exponential_backoff_interval,
                               humanize_seconds, localize, make_aware, maybe_iso8601, maybe_make_aware,
                               maybe_timedelta, rate, remaining, timezone, utcoffset)


class test_LocalTimezone:

    def test_daylight(self, patching):
        time = patching('celery.utils.time._time')
        time.timezone = 3600
        time.daylight = False
        x = LocalTimezone()
        assert x.STDOFFSET == timedelta(seconds=-3600)
        assert x.DSTOFFSET == x.STDOFFSET
        time.daylight = True
        time.altzone = 3600
        y = LocalTimezone()
        assert y.STDOFFSET == timedelta(seconds=-3600)
        assert y.DSTOFFSET == timedelta(seconds=-3600)

        assert repr(y)

        y._isdst = Mock()
        y._isdst.return_value = True
        assert y.utcoffset(datetime.now())
        assert not y.dst(datetime.now())
        y._isdst.return_value = False
        assert y.utcoffset(datetime.now())
        assert not y.dst(datetime.now())

        assert y.tzname(datetime.now())


class test_iso8601:

    def test_parse_with_timezone(self):
        d = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
        assert parse_iso8601(d.isoformat()) == d
        # 2013-06-07T20:12:51.775877+00:00
        iso = d.isoformat()
        iso1 = iso.replace('+00:00', '-01:00')
        d1 = parse_iso8601(iso1)
        d1_offset_in_minutes = d1.utcoffset().total_seconds() / 60
        assert d1_offset_in_minutes == -60
        iso2 = iso.replace('+00:00', '+01:00')
        d2 = parse_iso8601(iso2)
        d2_offset_in_minutes = d2.utcoffset().total_seconds() / 60
        assert d2_offset_in_minutes == +60
        iso3 = iso.replace('+00:00', 'Z')
        d3 = parse_iso8601(iso3)
        assert d3.tzinfo == _timezone.utc


@pytest.mark.parametrize('delta,expected', [
    (timedelta(days=2), datetime(2010, 3, 30, 0, 0)),
    (timedelta(hours=2), datetime(2010, 3, 30, 11, 0)),
    (timedelta(minutes=2), datetime(2010, 3, 30, 11, 50)),
    (timedelta(seconds=2), None),
])
def test_delta_resolution(delta, expected):
    dt = datetime(2010, 3, 30, 11, 50, 58, 41065)
    assert delta_resolution(dt, delta) == expected or dt


@pytest.mark.parametrize('seconds,expected', [
    (4 * 60 * 60 * 24, '4.00 days'),
    (1 * 60 * 60 * 24, '1.00 day'),
    (4 * 60 * 60, '4.00 hours'),
    (1 * 60 * 60, '1.00 hour'),
    (4 * 60, '4.00 minutes'),
    (1 * 60, '1.00 minute'),
    (4, '4.00 seconds'),
    (1, '1.00 second'),
    (4.3567631221, '4.36 seconds'),
    (0, 'now'),
])
def test_humanize_seconds(seconds, expected):
    assert humanize_seconds(seconds) == expected


def test_humanize_seconds__prefix():
    assert humanize_seconds(4, prefix='about ') == 'about 4.00 seconds'


def test_maybe_iso8601_datetime():
    now = datetime.now()
    assert maybe_iso8601(now) is now


@pytest.mark.parametrize('date_str,expected', [
    ('2011-11-04T00:05:23', datetime(2011, 11, 4, 0, 5, 23)),
    ('2011-11-04T00:05:23Z', datetime(2011, 11, 4, 0, 5, 23, tzinfo=_timezone.utc)),
    ('2011-11-04 00:05:23.283+00:00',
     datetime(2011, 11, 4, 0, 5, 23, 283000, tzinfo=_timezone.utc)),
    ('2011-11-04T00:05:23+04:00',
     datetime(2011, 11, 4, 0, 5, 23,  tzinfo=_timezone(timedelta(seconds=14400)))),
])
def test_iso8601_string_datetime(date_str, expected):
    assert maybe_iso8601(date_str) == expected


@pytest.mark.parametrize('arg,expected', [
    (30, timedelta(seconds=30)),
    (30.6, timedelta(seconds=30.6)),
    (timedelta(days=2), timedelta(days=2)),
])
def test_maybe_timedelta(arg, expected):
    assert maybe_timedelta(arg) == expected


def test_remaining():
    # Relative
    remaining(datetime.utcnow(), timedelta(hours=1), relative=True)

    """
    The upcoming cases check whether the next run is calculated correctly
    """
    eastern_tz = ZoneInfo("US/Eastern")
    tokyo_tz = ZoneInfo("Asia/Tokyo")

    # Case 1: `start` in UTC and `now` in other timezone
    start = datetime.now(ZoneInfo("UTC"))
    now = datetime.now(eastern_tz)
    delta = timedelta(hours=1)
    assert str(start.tzinfo) == str(ZoneInfo("UTC"))
    assert str(now.tzinfo) == str(eastern_tz)
    rem_secs = remaining(start, delta, now).total_seconds()
    # assert remaining time is approximately equal to delta
    assert rem_secs == pytest.approx(delta.total_seconds(), abs=1)

    # Case 2: `start` and `now` in different timezones (other than UTC)
    start = datetime.now(eastern_tz)
    now = datetime.now(tokyo_tz)
    delta = timedelta(hours=1)
    assert str(start.tzinfo) == str(eastern_tz)
    assert str(now.tzinfo) == str(tokyo_tz)
    rem_secs = remaining(start, delta, now).total_seconds()
    assert rem_secs == pytest.approx(delta.total_seconds(), abs=1)

    """
    Case 3: DST check
    Suppose start (which is last_run_time) is in EST while next_run is in EDT,
    then check whether the `next_run` is actually the time specified in the
    start (i.e. there is not an hour diff due to DST).
    In 2019, DST starts on March 10
    """
    start = datetime(
        month=3, day=9, year=2019, hour=10,
        minute=0, tzinfo=eastern_tz)  # EST

    now = datetime(
        day=11, month=3, year=2019, hour=1,
        minute=0, tzinfo=eastern_tz)  # EDT
    delta = ffwd(hour=10, year=2019, microsecond=0, minute=0,
                 second=0, day=11, weeks=0, month=3)
    # `next_actual_time` is the next time to run (derived from delta)
    next_actual_time = datetime(
        day=11, month=3, year=2019, hour=10, minute=0, tzinfo=eastern_tz)  # EDT
    assert start.tzname() == "EST"
    assert now.tzname() == "EDT"
    assert next_actual_time.tzname() == "EDT"
    rem_time = remaining(start, delta, now)
    next_run = now + rem_time
    assert next_run == next_actual_time


class test_timezone:

    def test_get_timezone_with_zoneinfo(self):
        assert timezone.get_timezone('UTC')

    def test_tz_or_local(self):
        assert timezone.tz_or_local() == timezone.local
        assert timezone.tz_or_local(timezone.utc)

    def test_to_local(self):
        assert timezone.to_local(make_aware(datetime.utcnow(), timezone.utc))
        assert timezone.to_local(datetime.utcnow())

    def test_to_local_fallback(self):
        assert timezone.to_local_fallback(
            make_aware(datetime.utcnow(), timezone.utc))
        assert timezone.to_local_fallback(datetime.utcnow())


class test_make_aware:

    def test_standard_tz(self):
        tz = tzinfo()
        wtz = make_aware(datetime.utcnow(), tz)
        assert wtz.tzinfo == tz

    def test_tz_when_zoneinfo(self):
        tz = ZoneInfo('US/Eastern')
        wtz = make_aware(datetime.utcnow(), tz)
        assert wtz.tzinfo == tz

    def test_maybe_make_aware(self):
        aware = datetime.utcnow().replace(tzinfo=timezone.utc)
        assert maybe_make_aware(aware)
        naive = datetime.utcnow()
        assert maybe_make_aware(naive)
        assert maybe_make_aware(naive).tzinfo is ZoneInfo("UTC")

        tz = ZoneInfo('US/Eastern')
        eastern = datetime.utcnow().replace(tzinfo=tz)
        assert maybe_make_aware(eastern).tzinfo is tz
        utcnow = datetime.utcnow()
        assert maybe_make_aware(utcnow, 'UTC').tzinfo is ZoneInfo("UTC")


class test_localize:

    def test_standard_tz(self):
        class tzz(tzinfo):

            def utcoffset(self, dt):
                return None  # Mock no utcoffset specified

        tz = tzz()
        assert localize(make_aware(datetime.utcnow(), tz), tz)

    @patch('dateutil.tz.datetime_ambiguous')
    def test_when_zoneinfo(self, datetime_ambiguous_mock):
        datetime_ambiguous_mock.return_value = False
        tz = ZoneInfo("US/Eastern")
        assert localize(make_aware(datetime.utcnow(), tz), tz)

        datetime_ambiguous_mock.return_value = True
        tz2 = ZoneInfo("US/Eastern")
        assert localize(make_aware(datetime.utcnow(), tz2), tz2)

    @patch('dateutil.tz.datetime_ambiguous')
    def test_when_is_ambiguous(self, datetime_ambiguous_mock):
        class tzz(tzinfo):

            def utcoffset(self, dt):
                return None  # Mock no utcoffset specified

            def is_ambiguous(self, dt):
                return True

        datetime_ambiguous_mock.return_value = False
        tz = tzz()
        assert localize(make_aware(datetime.utcnow(), tz), tz)

        datetime_ambiguous_mock.return_value = True
        tz2 = tzz()
        assert localize(make_aware(datetime.utcnow(), tz2), tz2)

    def test_localize_changes_utc_dt(self):
        now_utc_time = datetime.now(tz=ZoneInfo("UTC"))
        local_tz = ZoneInfo('US/Eastern')
        localized_time = localize(now_utc_time, local_tz)
        assert localized_time == now_utc_time

    def test_localize_aware_dt_idempotent(self):
        t = (2017, 4, 23, 21, 36, 59, 0)
        local_zone = ZoneInfo('America/New_York')
        local_time = datetime(*t)
        local_time_aware = datetime(*t, tzinfo=local_zone)
        alternate_zone = ZoneInfo('America/Detroit')
        localized_time = localize(local_time_aware, alternate_zone)
        assert localized_time == local_time_aware
        assert local_zone.utcoffset(
            local_time) == alternate_zone.utcoffset(local_time)
        localized_utc_offset = localized_time.tzinfo.utcoffset(local_time)
        assert localized_utc_offset == alternate_zone.utcoffset(local_time)
        assert localized_utc_offset == local_zone.utcoffset(local_time)


@pytest.mark.parametrize('s,expected', [
    (999, 999),
    (7.5, 7.5),
    ('2.5/s', 2.5),
    ('1456/s', 1456),
    ('100/m', 100 / 60.0),
    ('10/h', 10 / 60.0 / 60.0),
    (0, 0),
    (None, 0),
    ('0/m', 0),
    ('0/h', 0),
    ('0/s', 0),
    ('0.0/s', 0),
])
def test_rate_limit_string(s, expected):
    assert rate(s) == expected


class test_ffwd:

    def test_repr(self):
        x = ffwd(year=2012)
        assert repr(x)

    def test_radd_with_unknown_gives_NotImplemented(self):
        x = ffwd(year=2012)
        assert x.__radd__(object()) == NotImplemented


class test_utcoffset:

    def test_utcoffset(self, patching):
        _time = patching('celery.utils.time._time')
        _time.daylight = True
        assert utcoffset(time=_time) is not None
        _time.daylight = False
        assert utcoffset(time=_time) is not None


class test_get_exponential_backoff_interval:

    @patch('random.randrange', lambda n: n - 2)
    def test_with_jitter(self):
        assert get_exponential_backoff_interval(
            factor=4,
            retries=3,
            maximum=100,
            full_jitter=True
        ) == 4 * (2 ** 3) - 1

    def test_without_jitter(self):
        assert get_exponential_backoff_interval(
            factor=4,
            retries=3,
            maximum=100,
            full_jitter=False
        ) == 4 * (2 ** 3)

    def test_bound_by_maximum(self):
        maximum_boundary = 100
        assert get_exponential_backoff_interval(
            factor=40,
            retries=3,
            maximum=maximum_boundary
        ) == maximum_boundary

    @patch('random.randrange', lambda n: n - 1)
    def test_negative_values(self):
        assert get_exponential_backoff_interval(
            factor=-40,
            retries=3,
            maximum=100
        ) == 0

    @patch('random.randrange')
    def test_valid_random_range(self, rr):
        rr.return_value = 0
        maximum = 100
        get_exponential_backoff_interval(
            factor=40, retries=10, maximum=maximum, full_jitter=True)
        rr.assert_called_once_with(maximum + 1)
