import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pickle import dumps, loads
from unittest import TestCase
from unittest.mock import Mock

import pytest
from dateutil.relativedelta import relativedelta

from celery.exceptions import ImproperlyConfigured
from celery.schedules import ParseException, crontab, crontab_parser, schedule, solar

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


assertions = TestCase('__init__')


@contextmanager
def patch_crontab_nowfun(cls, retval):
    prev_nowfun = cls.nowfun
    cls.nowfun = lambda: retval
    try:
        yield
    finally:
        cls.nowfun = prev_nowfun


class test_solar:

    def setup_method(self):
        pytest.importorskip('ephem')
        self.s = solar('sunrise', 60, 30, app=self.app)

    def test_reduce(self):
        fun, args = self.s.__reduce__()
        assert fun(*args) == self.s

    def test_eq(self):
        assert self.s == solar('sunrise', 60, 30, app=self.app)
        assert self.s != solar('sunset', 60, 30, app=self.app)
        assert self.s != schedule(10)

    def test_repr(self):
        assert repr(self.s)

    def test_is_due(self):
        self.s.remaining_estimate = Mock(name='rem')
        self.s.remaining_estimate.return_value = timedelta(seconds=0)
        assert self.s.is_due(datetime.now(timezone.utc)).is_due

    def test_is_due__not_due(self):
        self.s.remaining_estimate = Mock(name='rem')
        self.s.remaining_estimate.return_value = timedelta(hours=10)
        assert not self.s.is_due(datetime.now(timezone.utc)).is_due

    def test_remaining_estimate(self):
        self.s.cal = Mock(name='cal')
        self.s.cal.next_rising().datetime.return_value = datetime.now(timezone.utc)
        self.s.remaining_estimate(datetime.now(timezone.utc))

    def test_coordinates(self):
        with pytest.raises(ValueError):
            solar('sunrise', -120, 60, app=self.app)
        with pytest.raises(ValueError):
            solar('sunrise', 120, 60, app=self.app)
        with pytest.raises(ValueError):
            solar('sunrise', 60, -200, app=self.app)
        with pytest.raises(ValueError):
            solar('sunrise', 60, 200, app=self.app)

    def test_invalid_event(self):
        with pytest.raises(ValueError):
            solar('asdqwewqew', 60, 60, app=self.app)

    def test_dusk_horizons_are_negative(self):
        """All dusk events should have negative horizons (sun below horizon)."""
        for event in ('dusk_civil', 'dusk_nautical', 'dusk_astronomical'):
            s = solar(event, 50, 10, app=self.app)
            horizon = float(s.cal.horizon)
            assert horizon < 0, (
                f"{event} horizon should be negative (below horizon), "
                f"got {s.cal.horizon}"
            )

    def test_event_uses_center(self):
        s = solar('solar_noon', 60, 60, app=self.app)
        for ev, is_center in s._use_center_l.items():
            s.method = s._methods[ev]
            s.is_center = s._use_center_l[ev]
            try:
                s.remaining_estimate(datetime.now(timezone.utc))
            except TypeError:
                pytest.fail(
                    f"{s.method} was called with 'use_center' which is not a "
                    "valid keyword for the function.")


class test_solar_without_ephem:

    def test_raises_improperly_configured_when_ephem_is_missing(
            self, monkeypatch):
        monkeypatch.setitem(sys.modules, 'ephem', None)
        with pytest.raises(ImproperlyConfigured, match=r'celery\[solar\]'):
            solar('sunrise', 60, 30, app=self.app)


class test_schedule:

    def test_ne(self):
        s1 = schedule(10, app=self.app)
        s2 = schedule(12, app=self.app)
        s3 = schedule(10, app=self.app)
        assert s1 == s3
        assert s1 != s2

    def test_pickle(self):
        s1 = schedule(10, app=self.app)
        fun, args = s1.__reduce__()
        s2 = fun(*args)
        assert s1 == s2


# Module-level helper used as crontab(nowfun=...) in pickling tests.
# Defined at top level so it is picklable/serializable.
def utcnow():
    return datetime.now(timezone.utc)


class test_crontab_parser:

    def crontab(self, *args, **kwargs):
        return crontab(*args, **dict(kwargs, app=self.app))

    def test_crontab_reduce(self):
        c = self.crontab('*')
        assert c == loads(dumps(c))
        c = self.crontab(
            minute='1',
            hour='2',
            day_of_week='3',
            day_of_month='4',
            month_of_year='5',
            nowfun=utcnow)
        assert c == loads(dumps(c))

    def test_crontab_reduce_preserves_state_across_multiple_round_trips(self):
        import functools

        c = self.crontab(nowfun=functools.partial(utcnow))

        for _ in range(5):
            c = loads(dumps(c))

        assert isinstance(c.nowfun, functools.partial)
        assert c.nowfun.func is utcnow

    def test_range_steps_not_enough(self):
        with pytest.raises(crontab_parser.ParseException):
            crontab_parser(24)._range_steps([1])

    def test_parse_star(self):
        assert crontab_parser(24).parse('*') == set(range(24))
        assert crontab_parser(60).parse('*') == set(range(60))
        assert crontab_parser(7).parse('*') == set(range(7))
        assert crontab_parser(31, 1).parse('*') == set(range(1, 31 + 1))
        assert crontab_parser(12, 1).parse('*') == set(range(1, 12 + 1))

    def test_parse_range(self):
        assert crontab_parser(60).parse('1-10') == set(range(1, 10 + 1))
        assert crontab_parser(24).parse('0-20') == set(range(0, 20 + 1))
        assert crontab_parser().parse('2-10') == set(range(2, 10 + 1))
        assert crontab_parser(60, 1).parse('1-10') == set(range(1, 10 + 1))

    def test_parse_range_wraps(self):
        assert crontab_parser(12).parse('11-1') == {11, 0, 1}
        assert crontab_parser(60, 1).parse('2-1') == set(range(1, 60 + 1))

    def test_parse_groups(self):
        assert crontab_parser().parse('1,2,3,4') == {1, 2, 3, 4}
        assert crontab_parser().parse('0,15,30,45') == {0, 15, 30, 45}
        assert crontab_parser(min_=1).parse('1,2,3,4') == {1, 2, 3, 4}

    def test_parse_steps(self):
        assert crontab_parser(8).parse('*/2') == {0, 2, 4, 6}
        assert crontab_parser().parse('*/2') == {i * 2 for i in range(30)}
        assert crontab_parser().parse('*/3') == {i * 3 for i in range(20)}
        assert crontab_parser(8, 1).parse('*/2') == {1, 3, 5, 7}
        assert crontab_parser(min_=1).parse('*/2') == {
            i * 2 + 1 for i in range(30)
        }
        assert crontab_parser(min_=1).parse('*/3') == {
            i * 3 + 1 for i in range(20)
        }

    def test_parse_composite(self):
        assert crontab_parser(8).parse('*/2') == {0, 2, 4, 6}
        assert crontab_parser().parse('2-9/5') == {2, 7}
        assert crontab_parser().parse('2-10/5') == {2, 7}
        assert crontab_parser(min_=1).parse('55-5/3') == {55, 58, 1, 4}
        assert crontab_parser().parse('2-11/5,3') == {2, 3, 7}
        assert crontab_parser().parse('2-4/3,*/5,0-21/4') == {
            0, 2, 4, 5, 8, 10, 12, 15, 16, 20, 25, 30, 35, 40, 45, 50, 55,
        }
        assert crontab_parser().parse('1-9/2') == {1, 3, 5, 7, 9}
        assert crontab_parser(8, 1).parse('*/2') == {1, 3, 5, 7}
        assert crontab_parser(min_=1).parse('2-9/5') == {2, 7}
        assert crontab_parser(min_=1).parse('2-10/5') == {2, 7}
        assert crontab_parser(min_=1).parse('2-11/5,3') == {2, 3, 7}
        assert crontab_parser(min_=1).parse('2-4/3,*/5,1-21/4') == {
            1, 2, 5, 6, 9, 11, 13, 16, 17, 21, 26, 31, 36, 41, 46, 51, 56,
        }
        assert crontab_parser(min_=1).parse('1-9/2') == {1, 3, 5, 7, 9}

    def test_parse_errors_on_empty_string(self):
        with pytest.raises(ParseException):
            crontab_parser(60).parse('')

    def test_parse_errors_on_empty_group(self):
        with pytest.raises(ParseException):
            crontab_parser(60).parse('1,,2')

    def test_parse_errors_on_empty_steps(self):
        with pytest.raises(ParseException):
            crontab_parser(60).parse('*/')

    def test_parse_errors_on_negative_number(self):
        with pytest.raises(ParseException):
            crontab_parser(60).parse('-20')

    def test_parse_errors_on_lt_min(self):
        crontab_parser(min_=1).parse('1')
        with pytest.raises(ValueError):
            crontab_parser(12, 1).parse('0')
        with pytest.raises(ValueError):
            crontab_parser(24, 1).parse('12-0')

    def test_parse_errors_on_gt_max(self):
        crontab_parser(1).parse('0')
        with pytest.raises(ValueError):
            crontab_parser(1).parse('1')
        with pytest.raises(ValueError):
            crontab_parser(60).parse('61-0')

    def test_expand_cronspec_eats_iterables(self):
        assert crontab._expand_cronspec(iter([1, 2, 3]), 100) == {1, 2, 3}
        assert crontab._expand_cronspec(iter([1, 2, 3]), 100, 1) == {1, 2, 3}

    def test_expand_cronspec_invalid_type(self):
        with pytest.raises(TypeError):
            crontab._expand_cronspec(object(), 100)

    def test_repr(self):
        assert '*' in repr(self.crontab('*'))

    def test_eq(self):
        assert (self.crontab(day_of_week='1, 2') ==
                self.crontab(day_of_week='1-2'))
        assert (self.crontab(day_of_month='1, 16, 31') ==
                self.crontab(day_of_month='*/15'))
        assert (
            self.crontab(
                minute='1', hour='2', day_of_week='5',
                day_of_month='10', month_of_year='5') ==
            self.crontab(
                minute='1', hour='2', day_of_week='5',
                day_of_month='10', month_of_year='5'))
        assert crontab(minute='1') != crontab(minute='2')
        assert (self.crontab(month_of_year='1') !=
                self.crontab(month_of_year='2'))
        assert object() != self.crontab(minute='1')
        assert self.crontab(minute='1') != object()
        assert crontab(month_of_year='1') != schedule(10)


class test_crontab_from_string:

    def test_every_minute(self):
        assert crontab.from_string('* * * * *') == crontab()

    def test_every_minute_on_sunday(self):
        assert crontab.from_string('* * * * SUN') == crontab(day_of_week='SUN')

    def test_once_per_month(self):
        assert crontab.from_string('0 8 5 * *') == crontab(minute=0, hour=8, day_of_month=5)

    def test_invalid_crontab_string(self):
        with pytest.raises(ValueError):
            crontab.from_string('*')


class test_crontab_remaining_estimate:

    def crontab(self, *args, **kwargs):
        return crontab(*args, **dict(kwargs, app=self.app))

    def next_occurrence(self, crontab, now):
        crontab.nowfun = lambda: now
        return now + crontab.remaining_estimate(now)

    def test_next_minute(self):
        next = self.next_occurrence(
            self.crontab(), datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 11, 14, 31)

    def test_not_next_minute(self):
        next = self.next_occurrence(
            self.crontab(), datetime(2010, 9, 11, 14, 59, 15),
        )
        assert next == datetime(2010, 9, 11, 15, 0)

    def test_this_hour(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42]), datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 11, 14, 42)

    def test_not_this_hour(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 10, 15]),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 11, 15, 5)

    def test_today(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], hour=[12, 17]),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 11, 17, 5)

    def test_not_today(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], hour=[12]),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 12, 12, 5)

    def test_weekday(self):
        next = self.next_occurrence(
            self.crontab(minute=30, hour=14, day_of_week='sat'),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 18, 14, 30)

    def test_not_weekday(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='mon-fri'),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 13, 0, 5)

    def test_monthyear(self):
        next = self.next_occurrence(
            self.crontab(minute=30, hour=14, month_of_year='oct', day_of_month=18),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 10, 18, 14, 30)

    def test_not_monthyear(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], month_of_year='nov-dec', day_of_month=13),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 11, 13, 0, 5)

    def test_monthday(self):
        next = self.next_occurrence(
            self.crontab(minute=30, hour=14, day_of_month=18),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 18, 14, 30)

    def test_not_monthday(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_month=29),
            datetime(2010, 1, 22, 14, 30, 15),
        )
        assert next == datetime(2010, 1, 29, 0, 5)

    def test_weekday_monthday(self):
        next = self.next_occurrence(
            self.crontab(minute=30, hour=14,
                         day_of_week='mon', day_of_month=18),
            datetime(2010, 1, 18, 14, 30, 15),
        )
        assert next == datetime(2010, 10, 18, 14, 30)

    def test_monthday_not_weekday(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='sat', day_of_month=29),
            datetime(2010, 1, 29, 0, 5, 15),
        )
        assert next == datetime(2010, 5, 29, 0, 5)

    def test_weekday_not_monthday(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='mon', day_of_month=18),
            datetime(2010, 1, 11, 0, 5, 15),
        )
        assert next == datetime(2010, 1, 18, 0, 5)

    def test_not_weekday_not_monthday(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='mon', day_of_month=18),
            datetime(2010, 1, 10, 0, 5, 15),
        )
        assert next == datetime(2010, 1, 18, 0, 5)

    def test_leapday(self):
        next = self.next_occurrence(
            self.crontab(minute=30, hour=14, day_of_month=29),
            datetime(2012, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2012, 2, 29, 14, 30)

    def test_not_leapday(self):
        next = self.next_occurrence(
            self.crontab(minute=30, hour=14, day_of_month=29),
            datetime(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2010, 3, 29, 14, 30)

    def test_weekmonthdayyear(self):
        next = self.next_occurrence(
            self.crontab(minute=30, hour=14, day_of_week='fri',
                         day_of_month=29, month_of_year=1),
            datetime(2010, 1, 22, 14, 30, 15),
        )
        assert next == datetime(2010, 1, 29, 14, 30)

    def test_monthdayyear_not_week(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='wed,thu',
                         day_of_month=29, month_of_year='1,4,7'),
            datetime(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2010, 4, 29, 0, 5)

    def test_weekdaymonthyear_not_monthday(self):
        next = self.next_occurrence(
            self.crontab(minute=30, hour=14, day_of_week='fri',
                         day_of_month=29, month_of_year='1-10'),
            datetime(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2010, 10, 29, 14, 30)

    def test_weekmonthday_not_monthyear(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='fri',
                         day_of_month=29, month_of_year='2-10'),
            datetime(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2010, 10, 29, 0, 5)

    def test_weekday_not_monthdayyear(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='mon',
                         day_of_month=18, month_of_year='2-10'),
            datetime(2010, 1, 11, 0, 5, 15),
        )
        assert next == datetime(2010, 10, 18, 0, 5)

    def test_monthday_not_weekdaymonthyear(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='mon',
                         day_of_month=29, month_of_year='2-4'),
            datetime(2010, 1, 29, 0, 5, 15),
        )
        assert next == datetime(2010, 3, 29, 0, 5)

    def test_monthyear_not_weekmonthday(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='mon',
                         day_of_month=29, month_of_year='2-4'),
            datetime(2010, 2, 28, 0, 5, 15),
        )
        assert next == datetime(2010, 3, 29, 0, 5)

    def test_not_weekmonthdayyear(self):
        next = self.next_occurrence(
            self.crontab(minute=[5, 42], day_of_week='fri,sat',
                         day_of_month=29, month_of_year='2-10'),
            datetime(2010, 1, 28, 14, 30, 15),
        )
        assert next == datetime(2010, 5, 29, 0, 5)

    def test_invalid_specification(self):
        # *** WARNING ***
        # This test triggers an infinite loop in case of a regression
        with pytest.raises(RuntimeError):
            self.next_occurrence(
                self.crontab(day_of_month=31, month_of_year=4),
                datetime(2010, 1, 28, 14, 30, 15),
            )

    def test_leapyear(self):
        next = self.next_occurrence(
            self.crontab(minute=30, hour=14, day_of_month=29, month_of_year=2),
            datetime(2012, 2, 29, 14, 30),
        )
        assert next == datetime(2016, 2, 29, 14, 30)

    def test_day_after_dst_end(self):
        # Test for #1604 issue with region configuration using DST
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        crontab = self.crontab(minute=0, hour=9)

        # Set last_run_at Before DST end
        last_run_at = datetime(2017, 10, 28, 9, 0, tzinfo=tz)
        # Set now after DST end
        now = datetime(2017, 10, 29, 7, 0, tzinfo=tz)
        crontab.nowfun = lambda: now
        next = now + crontab.remaining_estimate(last_run_at)

        assert next == datetime(2017, 10, 29, 9, 0, tzinfo=tz)
        assert next.utcoffset().seconds == 3600

    def test_day_after_dst_start(self):
        # Test for #1604 issue with region configuration using DST
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        crontab = self.crontab(minute=0, hour=9)

        # Set last_run_at Before DST start
        last_run_at = datetime(2017, 3, 25, 9, 0, tzinfo=tz)
        # Set now after DST start
        now = datetime(2017, 3, 26, 7, 0, tzinfo=tz)
        crontab.nowfun = lambda: now
        next = now + crontab.remaining_estimate(last_run_at)

        assert next.utcoffset().seconds == 7200
        assert next == datetime(2017, 3, 26, 9, 0, tzinfo=tz)

    def test_negative_utc_timezone_with_day_of_month(self):
        # UTC-8
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)

        # set day_of_month to test on _delta_to_next
        crontab = self.crontab(minute=0, day_of_month='27-31')

        # last_run_at: '2023/01/28T23:00:00-08:00'
        last_run_at = datetime(2023, 1, 28, 23, 0, tzinfo=tz)

        # now: '2023/01/29T00:00:00-08:00'
        now = datetime(2023, 1, 29, 0, 0, tzinfo=tz)

        crontab.nowfun = lambda: now
        next = now + crontab.remaining_estimate(last_run_at)

        assert next == datetime(2023, 1, 29, 0, 0, tzinfo=tz)

    def test_aware_last_run_at_in_different_timezone(self):
        # The crontab fields are defined in the schedule's timezone (the app
        # timezone, UTC here), but an aware last_run_at may arrive in a
        # different timezone, e.g. from django-celery-beat.  Both datetimes
        # must be normalized into the schedule's frame before any field
        # matching (#9715).
        vilnius = ZoneInfo("Europe/Vilnius")
        crontab = self.crontab(minute=40, hour=8)

        # 09:25:08 in Vilnius == 06:25:08 UTC
        last_run_at = datetime(2025, 5, 20, 9, 25, 8, tzinfo=vilnius)
        now = datetime(2025, 5, 20, 9, 26, 8, tzinfo=vilnius)
        crontab.nowfun = lambda: now

        next = now + crontab.remaining_estimate(last_run_at)

        # The next run is at 08:40 UTC on the same day, not a day later.
        assert next == datetime(2025, 5, 20, 8, 40, tzinfo=ZoneInfo("UTC"))

    def test_aware_last_run_at_in_different_timezone_without_utc(self):
        # Same as above with enable_utc off, which is a common
        # django-celery-beat setup.  The returned datetimes must stay in the
        # frame the delta was computed in (#9715).
        self.app.conf.enable_utc = False
        self.app.conf.timezone = "UTC"
        vilnius = ZoneInfo("Europe/Vilnius")
        crontab = self.crontab(minute=40, hour=8)

        last_run_at = datetime(2025, 5, 20, 9, 25, 8, tzinfo=vilnius)
        now = datetime(2025, 5, 20, 9, 26, 8, tzinfo=vilnius)
        crontab.nowfun = lambda: now

        next = now + crontab.remaining_estimate(last_run_at)

        assert next == datetime(2025, 5, 20, 8, 40, tzinfo=ZoneInfo("UTC"))

    def test_remaining_estimate_finds_hour_slot_before_now(self):
        crontab = self.crontab(minute=0, hour='1,2')  # every day at 01:00 and 02:00
        last_run_at = datetime(2022, 12, 5, 1, 0)  # next run is 02:00
        now = datetime(2022, 12, 6, 0, 10)  # the next day
        crontab.nowfun = lambda: now

        next = now + crontab.remaining_estimate(last_run_at)

        assert next == datetime(2022, 12, 5, 2, 0)

    def test_remaining_estimate_finds_minute_slot_before_now(self):
        crontab = self.crontab(minute='0,30', hour=1)  # every day at 01:00 and 01:30
        last_run_at = datetime(2022, 12, 5, 1, 0)  # next run is 01:30
        now = datetime(2022, 12, 6, 0, 10)  # the next day
        crontab.nowfun = lambda: now

        next = now + crontab.remaining_estimate(last_run_at)

        assert next == datetime(2022, 12, 5, 1, 30)

    def test_minute_scheduling_remaining_estimate_during_dst_start(self):
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        ct = crontab(app=self.app)
        # Paris DST start 2024-03-31T03:00:00+02:00 + 1 minute
        last_run_at = (datetime(2024, 3, 31, 1, 0, tzinfo=ZoneInfo("UTC")) + timedelta(minutes=1)).astimezone(tz)
        now = last_run_at
        ct.nowfun = lambda: now

        assert ct.remaining_estimate(last_run_at).total_seconds() == 60

    def test_minute_scheduling_remaining_estimate_during_dst_end(self):
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        ct = crontab(app=self.app)
        # # Paris DST end 2024-10-27T02:00:00+01:00 + 1 minute
        last_run_at = (datetime(2024, 10, 27, 1, 0, tzinfo=ZoneInfo("UTC")) + timedelta(minutes=1)).astimezone(tz)
        now = last_run_at
        ct.nowfun = lambda: now

        assert ct.remaining_estimate(last_run_at).total_seconds() == 60


class test_crontab_remaining_estimate_with_timezone:
    # dst dates are setup in UTC so we can safely add timedelta for tests parametrization

    # 2024-03-10T03:00:00-07:00 -> 2024-03-10T10:00:00+00:00
    los_angeles_dst_start = datetime(2024, 3, 10, 10, 0, tzinfo=ZoneInfo("UTC"))

    # 2024-11-03T01:00:00-08:00 -> 2024-11-03T09:00:00+00:00
    los_angeles_dst_end = datetime(2024, 11, 3, 9, 0, tzinfo=ZoneInfo("UTC"))

    # 2024-03-31T03:00:00+02:00 -> 2024-03-31T01:00:00+00:00
    paris_dst_start = datetime(2024, 3, 31, 1, 0, tzinfo=ZoneInfo("UTC"))

    # 2024-10-27T02:00:00+01:00 -> 2024-10-27T01:00:00+00:00
    paris_dst_end = datetime(2024, 10, 27, 1, 0, tzinfo=ZoneInfo("UTC"))

    # 2026-09-06T01:00:00-03:00 -> 2026-09-06T04:00:00+00:00
    santiago_dst_start = datetime(2026, 9, 6, 4, 0, tzinfo=ZoneInfo("UTC"))

    # 2026-04-04T23:00:00-04:00 -> 2026-09-05T03:00:00+00:00
    santiago_dst_end = datetime(2026, 4, 5, 3, 0, tzinfo=ZoneInfo("UTC"))

    DST_CHANGE = {
        "America/Los_Angeles": {
            "start": los_angeles_dst_start,
            "end": los_angeles_dst_end,
        },
        "Europe/Paris": {
            "start": paris_dst_start,
            "end": paris_dst_end,
        },
        "America/Santiago": {
            "start": santiago_dst_start,
            "end": santiago_dst_end,
        }
    }

    def test_minute_scheduling_remaining_estimate_during_dst_start(self):
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        ct = crontab(app=self.app)
        # last_run_at = self.paris_dst_start + timedelta(minutes=1)
        last_run_at = (datetime(2024, 3, 31, 1, 0, tzinfo=ZoneInfo("UTC")) + timedelta(minutes=1)).astimezone(tz)
        now = last_run_at
        ct.nowfun = lambda: now

        assert ct.remaining_estimate(last_run_at).total_seconds() == 60

    def test_minute_scheduling_remaining_estimate_during_dst_end(self):
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        ct = crontab(app=self.app)
        # last_run_at = self.paris_dst_end + timedelta(minutes=1)
        last_run_at = (datetime(2024, 10, 27, 1, 0, tzinfo=ZoneInfo("UTC")) + timedelta(minutes=1)).astimezone(tz)
        now = last_run_at
        ct.nowfun = lambda: now

        assert ct.remaining_estimate(last_run_at).total_seconds() == 60

    def test_fixed_hour_scheduling_on_dst_start_new_hour(self):
        """DST start makes midnight become 1h and the cron is scheduled at 01:00"""
        tz = ZoneInfo("America/Santiago")
        self.app.timezone = tz
        ct = crontab(minute=0, hour=1, app=self.app)

        now = (self.santiago_dst_start - timedelta(hours=1)).astimezone(tz)
        ct.nowfun = lambda: now
        assert ct.remaining_estimate(now).total_seconds() == 60 * 60

        now = (self.santiago_dst_start).astimezone(tz)
        ct.nowfun = lambda: now
        assert ct.remaining_estimate(now).total_seconds() == 24 * 60 * 60

    @pytest.mark.parametrize(
        "tzname",
        [
            "America/Los_Angeles",
            "Europe/Paris",
            "America/Santiago",  # Chile DST change happens at midnight local time
        ]
    )
    @pytest.mark.parametrize(
        ("cron", "dst_start_or_end", "last_run_delta", "now_run_delta", "expected_sec"),
        [
            # scheduled every minute
            # last_run 1 minute before DST change, now at DST change
            ({}, "start", timedelta(minutes=-1), timedelta(), 0),
            ({}, "end", timedelta(minutes=-1), timedelta(), 0),

            # scheduled every minute
            # last_run at DST change, now at DST change + 1 minute
            ({}, "start", timedelta(), timedelta(minutes=1), 0),
            ({}, "end", timedelta(), timedelta(minutes=1), 0),

            # scheduled every hour at minute 0
            # last run one hour before DST change, now at DST change
            ({"minute": 0}, "start", timedelta(hours=-1), timedelta(), 0),
            ({"minute": 0}, "end", timedelta(hours=-1), timedelta(), 0),

            # scheduled every hour at minute 0
            # last run one hour before DST change, now 30 minutes before DST change
            ({"minute": 0}, "start", timedelta(hours=-1), timedelta(minutes=-30), 30 * 60),
            ({"minute": 0}, "end", timedelta(hours=-1), timedelta(minutes=-30), 30 * 60),

            # scheduled every hour at minute 0
            # last run one at DST change, now 30 minutes after DST change
            ({"minute": 0}, "start", timedelta(), timedelta(minutes=30), 30 * 60),
            ({"minute": 0}, "end", timedelta(), timedelta(minutes=30), 30 * 60),

            # scheduled every hour at minute 0
            # last run one at DST change, now 1 hour after DST change
            ({"minute": 0}, "start", timedelta(), timedelta(hours=1), 0),
            ({"minute": 0}, "end", timedelta(), timedelta(hours=1), 0),

            # scheduled every hour at minute 30
            # last run 30 minutes before at DST change, now at DST change
            ({"minute": 30}, "start", timedelta(minutes=-30), timedelta(), 30 * 60),
            ({"minute": 30}, "end", timedelta(minutes=-30), timedelta(), 30 * 60),

            # scheduled every hour at minute 30
            # last run 30 minutes before at DST change, now 30 minutes after DST change
            ({"minute": 30}, "start", timedelta(minutes=-30), timedelta(minutes=30), 0),
            ({"minute": 30}, "end", timedelta(minutes=-30), timedelta(minutes=30), 0),

            # scheduled every hour at minute 30
            # last run 30 minutes before at DST change, now at DST change
            ({"minute": "*/30"}, "start", timedelta(minutes=-30), timedelta(), 0),
            ({"minute": "*/30"}, "end", timedelta(minutes=-30), timedelta(), 0),

            # scheduled every hour at minute 30
            # last run at DST change, now 30 minutes after DST change
            ({"minute": "*/30"}, "start", timedelta(), timedelta(minutes=30), 0),
            ({"minute": "*/30"}, "end", timedelta(), timedelta(minutes=30), 0),
        ],
        ids=lambda x: f"{crontab(**x)}" if isinstance(x, dict) else x
    )
    def test_crontab_with_timezone_remaining_seconds_from_now(
        self, cron, tzname, dst_start_or_end, last_run_delta, now_run_delta, expected_sec
    ):
        """now != last_run, this simulates the computation of the remaining_seconds from now until next_run."""
        tz = ZoneInfo(tzname)
        self.app.timezone = tz
        dst_change_for_tz_in_utc = self.DST_CHANGE[tzname][dst_start_or_end]

        if isinstance(last_run_delta, relativedelta):
            # apply the relativedelta to the datetime in the final timezone to simplify setup
            # should only be used if the delta is big enough to not finish in the dst change zone
            last_run = dst_change_for_tz_in_utc.astimezone(tz) + last_run_delta
        else:
            # apply timedelta to the utc version of date so we can move around the dst change time safely
            last_run = (dst_change_for_tz_in_utc + last_run_delta).astimezone(tz)

        if isinstance(now_run_delta, relativedelta):
            # apply the relativedelta to the datetime in the final timezone to simplify setup
            # should only be used if the delta is big enough to not finish in the dst change zone
            now = dst_change_for_tz_in_utc.astimezone(tz) + now_run_delta
        else:
            # apply timedelta to the utc version of date so we can move around the dst change time safely
            now = (dst_change_for_tz_in_utc + now_run_delta).astimezone(tz)

        ct = crontab(**cron, app=self.app)
        ct.nowfun = lambda: now

        assert ct.remaining_estimate(last_run).total_seconds() == expected_sec

    @pytest.mark.parametrize(
        "tzname",
        [
            "America/Los_Angeles",
            "Europe/Paris",
            "America/Santiago",  # Chile DST change happens at midnight local time
        ]
    )
    @pytest.mark.parametrize(
        ("cron", "dst_start_or_end", "delta_to_dst_change", "expected_sec"),
        [
            # scheduled every minute, 1 minute before DST change
            ({}, "start", timedelta(minutes=-1), 60),
            ({}, "end", timedelta(minutes=-1), 60),

            # scheduled every minute, at DST change
            ({}, "start", timedelta(), 60),
            ({}, "end", timedelta(), 60),

            # scheduled every hour at minute 0, one hour before DST change
            ({"minute": 0}, "start", timedelta(hours=-1), 3600),
            ({"minute": 0}, "end", timedelta(hours=-1), 3600),

            # scheduled every hour at minute 0, at DST change
            ({"minute": 0}, "start", timedelta(), 3600),
            ({"minute": 0}, "end", timedelta(), 3600),

            # scheduled every hour at minute 30, 30 minutes before DST change
            ({"minute": 30}, "start", timedelta(minutes=-30), 3600),
            ({"minute": 30}, "end", timedelta(minutes=-30), 3600),

            # scheduled every hour at minute 30, 1 minutes before DST change
            ({"minute": 30}, "start", timedelta(minutes=-1), 1860),
            ({"minute": 30}, "end", timedelta(minutes=-1), 1860),

            # scheduled every hour at minute 30, at DST change
            ({"minute": 30}, "start", timedelta(), 1800),
            ({"minute": 30}, "end", timedelta(), 1800),

            # scheduled every hour at minute 30, 30 minutes after DST change
            ({"minute": 30}, "start", timedelta(minutes=30), 3600),
            ({"minute": 30}, "end", timedelta(minutes=30), 3600),

            # scheduled every 15 minutes, last run 15 minutes before DST change
            ({"minute": "*/15"}, "start", timedelta(minutes=-15), 15 * 60),
            ({"minute": "*/15"}, "end", timedelta(minutes=-15), 15 * 60),

            # scheduled every 15 minutes, at DST change
            ({"minute": "*/15"}, "start", timedelta(), 15 * 60),
            ({"minute": "*/15"}, "end", timedelta(), 15 * 60),

            # scheduled every day at noon, last run the day before DST change
            # there is one hour less until next_run
            ({"hour": 12, "minute": 0}, "start", relativedelta(days=-1, hour=12), 23 * 60 * 60),
            # there is one hour more until next_run
            ({"hour": 12, "minute": 0}, "end", relativedelta(days=-1, hour=12), 25 * 60 * 60),

            # scheduled every day at midnight, last run the day before DST change
            # there is one hour less until next_run
            ({"hour": 0, "minute": 0}, "start", relativedelta(hour=0), 23 * 60 * 60),
            # there is one hour more until next_run
            ({"hour": 0, "minute": 0}, "end", relativedelta(hour=0), 25 * 60 * 60),
        ],
        ids=lambda x: f"{x}" if isinstance(x, dict) else x
    )
    def test_crontab_with_timezone_remaining_seconds(
        self, cron, tzname, dst_start_or_end, delta_to_dst_change, expected_sec
    ):
        """now == last_run, this simulates the computation of the remaining_seconds until next_run."""
        tz = ZoneInfo(tzname)
        self.app.timezone = tz
        dst_change_for_tz_in_utc = self.DST_CHANGE[tzname][dst_start_or_end]

        if isinstance(delta_to_dst_change, relativedelta):
            # apply the relativedelta manually to the datetime in the final timezone to simplify setup
            # should only be used if the delta is big enough to not finish in the dst change zone
            if delta_to_dst_change.days is not None:
                last_run = (dst_change_for_tz_in_utc + relativedelta(days=delta_to_dst_change.days)).astimezone(tz)
            else:
                last_run = dst_change_for_tz_in_utc.astimezone(tz)
            if delta_to_dst_change.hour is not None:
                last_run = last_run.replace(hour=delta_to_dst_change.hour)
            if delta_to_dst_change.minute is not None:
                last_run = last_run.replace(minute=delta_to_dst_change.minute)
        else:
            # apply timedelta to the utc version of date so we can move around the dst change time safely
            last_run = (dst_change_for_tz_in_utc + delta_to_dst_change).astimezone(tz)
        now = last_run

        ct = crontab(**cron, app=self.app)
        ct.nowfun = lambda: now

        assert ct.remaining_estimate(last_run).total_seconds() == expected_sec

    def test_imaginary_hour(self):
        """verify crontab is skipped when the hour does not exists due to DST end."""
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        # last run the previous day
        last_run = self.paris_dst_start.astimezone(tz) + relativedelta(days=-1, hour=2)

        # now just 1 minute before dst change
        now = (self.paris_dst_start + timedelta(minutes=-1)).astimezone(tz)

        # every day at 2
        ct = crontab(minute=0, hour=2, app=self.app)
        ct.nowfun = lambda: now

        # expected next run is one day further, as the 2 o'clock hour does not exist on the day of dst start
        # next day but one hour less because of time shift
        assert (
            ct.remaining_estimate(last_run).total_seconds()
            == timedelta(days=1, hours=-1, minutes=1).total_seconds()
        )

        # now at dst change
        now = self.paris_dst_start.astimezone(tz)
        ct.nowfun = lambda: now

        # it is now 3, expected next run is one day
        assert ct.remaining_estimate(last_run).total_seconds() == timedelta(days=1, hours=-1).total_seconds()

    def test_duplicated_hour(self):
        """verify crontab is due twice when the hour is duplicated due to DST end."""
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        # last run the previous day
        last_run = self.paris_dst_end.astimezone(tz) + relativedelta(days=-1, hour=2)

        # now just 1 hour before dst change
        now = (self.paris_dst_end + timedelta(hours=-1)).astimezone(tz)
        # every day at 2
        ct = crontab(minute=0, hour=2, app=self.app)
        ct.nowfun = lambda: now

        # one hour before the dst end, it is 2 o'clock, due time is 0
        assert ct.remaining_estimate(last_run).total_seconds() == 0

        last_run = now
        # now at dst change, it is still 2 o'clock, due time should be 0
        now = self.paris_dst_end.astimezone(tz)

        ct.nowfun = lambda: now

        assert ct.remaining_estimate(last_run).total_seconds() == 0


class test_crontab_is_due:

    def setup_method(self):
        self.now = self.app.now()
        self.next_minute = 60 - self.now.second - 1e-6 * self.now.microsecond
        self.every_minute = self.crontab()
        self.quarterly = self.crontab(minute='*/15')
        self.hourly = self.crontab(minute=30)
        self.daily = self.crontab(hour=7, minute=30)
        self.weekly = self.crontab(hour=7, minute=30, day_of_week='thursday')
        self.monthly = self.crontab(
            hour=7, minute=30, day_of_week='thursday', day_of_month='8-14',
        )
        self.monthly_moy = self.crontab(
            hour=22, day_of_week='*', month_of_year='2',
            day_of_month='26,27,28',
        )
        self.yearly = self.crontab(
            hour=7, minute=30, day_of_week='thursday',
            day_of_month='8-14', month_of_year=3,
        )

    def crontab(self, *args, **kwargs):
        return crontab(*args, app=self.app, **kwargs)

    def test_default_crontab_spec(self):
        c = self.crontab()
        assert c.minute == set(range(60))
        assert c.hour == set(range(24))
        assert c.day_of_week == set(range(7))
        assert c.day_of_month == set(range(1, 32))
        assert c.month_of_year == set(range(1, 13))

    def test_simple_crontab_spec(self):
        c = self.crontab(minute=30)
        assert c.minute == {30}
        assert c.hour == set(range(24))
        assert c.day_of_week == set(range(7))
        assert c.day_of_month == set(range(1, 32))
        assert c.month_of_year == set(range(1, 13))

    @pytest.mark.parametrize('minute,expected', [
        (30, {30}),
        ('30', {30}),
        ((30, 40, 50), {30, 40, 50}),
        ((30, 40, 50, 51), {30, 40, 50, 51})
    ])
    def test_crontab_spec_minute_formats(self, minute, expected):
        c = self.crontab(minute=minute)
        assert c.minute == expected

    @pytest.mark.parametrize('minute', [60, '0-100'])
    def test_crontab_spec_invalid_minute(self, minute):
        with pytest.raises(ValueError):
            self.crontab(minute=minute)

    @pytest.mark.parametrize('hour,expected', [
        (6, {6}),
        ('5', {5}),
        ((4, 8, 12), {4, 8, 12}),
    ])
    def test_crontab_spec_hour_formats(self, hour, expected):
        c = self.crontab(hour=hour)
        assert c.hour == expected

    @pytest.mark.parametrize('hour', [24, '0-30'])
    def test_crontab_spec_invalid_hour(self, hour):
        with pytest.raises(ValueError):
            self.crontab(hour=hour)

    @pytest.mark.parametrize('day_of_week,expected', [
        (5, {5}),
        ('5', {5}),
        ('fri', {5}),
        ('tuesday,sunday,fri', {0, 2, 5}),
        ('mon-fri', {1, 2, 3, 4, 5}),
        ('*/2', {0, 2, 4, 6}),
    ])
    def test_crontab_spec_dow_formats(self, day_of_week, expected):
        c = self.crontab(day_of_week=day_of_week)
        assert c.day_of_week == expected

    @pytest.mark.parametrize('day_of_week', [
        'fooday-barday', '1,4,foo', '7', '12',
    ])
    def test_crontab_spec_invalid_dow(self, day_of_week):
        with pytest.raises(ValueError):
            self.crontab(day_of_week=day_of_week)

    @pytest.mark.parametrize('day_of_month,expected', [
        (5, {5}),
        ('5', {5}),
        ('2,4,6', {2, 4, 6}),
        ('*/5', {1, 6, 11, 16, 21, 26, 31}),
    ])
    def test_crontab_spec_dom_formats(self, day_of_month, expected):
        c = self.crontab(day_of_month=day_of_month)
        assert c.day_of_month == expected

    @pytest.mark.parametrize('day_of_month', [0, '0-10', 32, '31,32'])
    def test_crontab_spec_invalid_dom(self, day_of_month):
        with pytest.raises(ValueError):
            self.crontab(day_of_month=day_of_month)

    @pytest.mark.parametrize('month_of_year,expected', [
        (1, {1}),
        ('1', {1}),
        ('feb', {2}),
        ('Mar', {3}),
        ('april', {4}),
        ('may,jun,jul', {5, 6, 7}),
        ('aug-oct', {8, 9, 10}),
        ('2,4,6', {2, 4, 6}),
        ('*/2', {1, 3, 5, 7, 9, 11}),
        ('2-12/2', {2, 4, 6, 8, 10, 12}),
    ])
    def test_crontab_spec_moy_formats(self, month_of_year, expected):
        c = self.crontab(month_of_year=month_of_year)
        assert c.month_of_year == expected

    @pytest.mark.parametrize('month_of_year', [0, '0-5', 13, '12,13', 'jaan', 'sebtember'])
    def test_crontab_spec_invalid_moy(self, month_of_year):
        with pytest.raises(ValueError):
            self.crontab(month_of_year=month_of_year)

    def seconds_almost_equal(self, a, b, precision):
        for index, skew in enumerate((+1, -1, 0)):
            try:
                assertions.assertAlmostEqual(a, b + skew, precision)
            except Exception as exc:
                # AssertionError != builtins.AssertionError in pytest
                if 'AssertionError' in str(exc):
                    if index + 1 >= 3:
                        raise
            else:
                break

    def test_every_minute_execution_is_due(self):
        last_ran = self.now - timedelta(seconds=61)
        due, remaining = self.every_minute.is_due(last_ran)
        self.assert_relativedelta(self.every_minute, last_ran)
        assert due
        self.seconds_almost_equal(remaining, self.next_minute, 1)

    def assert_relativedelta(self, due, last_ran):
        try:
            from dateutil.relativedelta import relativedelta
        except ImportError:
            return
        l1, d1, n1 = due.remaining_delta(last_ran)
        l2, d2, n2 = due.remaining_delta(last_ran, ffwd=relativedelta)
        if not isinstance(d1, relativedelta):
            assert l1 == l2
            for field, value in d1._fields().items():
                assert getattr(d1, field) == value
            assert not d2.years
            assert not d2.months
            assert not d2.days
            assert not d2.leapdays
            assert not d2.hours
            assert not d2.minutes
            assert not d2.seconds
            assert not d2.microseconds

    def test_every_minute_execution_is_not_due(self):
        last_ran = self.now - timedelta(seconds=self.now.second)
        due, remaining = self.every_minute.is_due(last_ran)
        assert not due
        self.seconds_almost_equal(remaining, self.next_minute, 1)

    def test_execution_is_due_on_saturday(self):
        # 29th of May 2010 is a saturday
        with patch_crontab_nowfun(self.hourly, datetime(2010, 5, 29, 10, 30)):
            last_ran = self.now - timedelta(seconds=61)
            due, remaining = self.every_minute.is_due(last_ran)
            assert due
            self.seconds_almost_equal(remaining, self.next_minute, 1)

    def test_execution_is_due_on_sunday(self):
        # 30th of May 2010 is a sunday
        with patch_crontab_nowfun(self.hourly, datetime(2010, 5, 30, 10, 30)):
            last_ran = self.now - timedelta(seconds=61)
            due, remaining = self.every_minute.is_due(last_ran)
            assert due
            self.seconds_almost_equal(remaining, self.next_minute, 1)

    def test_execution_is_due_on_monday(self):
        # 31st of May 2010 is a monday
        with patch_crontab_nowfun(self.hourly, datetime(2010, 5, 31, 10, 30)):
            last_ran = self.now - timedelta(seconds=61)
            due, remaining = self.every_minute.is_due(last_ran)
            assert due
            self.seconds_almost_equal(remaining, self.next_minute, 1)

    def test_every_hour_execution_is_due(self):
        with patch_crontab_nowfun(self.hourly, datetime(2010, 5, 10, 10, 30)):
            due, remaining = self.hourly.is_due(datetime(2010, 5, 10, 6, 30))
            assert due
            assert remaining == 60 * 60

    def test_every_hour_execution_is_not_due(self):
        with patch_crontab_nowfun(self.hourly, datetime(2010, 5, 10, 10, 29)):
            due, remaining = self.hourly.is_due(datetime(2010, 5, 10, 9, 30))
            assert not due
            assert remaining == 60

    def test_first_quarter_execution_is_due(self):
        with patch_crontab_nowfun(
                self.quarterly, datetime(2010, 5, 10, 10, 15)):
            due, remaining = self.quarterly.is_due(
                datetime(2010, 5, 10, 6, 30),
            )
            assert due
            assert remaining == 15 * 60

    def test_second_quarter_execution_is_due(self):
        with patch_crontab_nowfun(
                self.quarterly, datetime(2010, 5, 10, 10, 30)):
            due, remaining = self.quarterly.is_due(
                datetime(2010, 5, 10, 6, 30),
            )
            assert due
            assert remaining == 15 * 60

    def test_first_quarter_execution_is_not_due(self):
        with patch_crontab_nowfun(
                self.quarterly, datetime(2010, 5, 10, 10, 14)):
            due, remaining = self.quarterly.is_due(
                datetime(2010, 5, 10, 10, 0),
            )
            assert not due
            assert remaining == 60

    def test_second_quarter_execution_is_not_due(self):
        with patch_crontab_nowfun(
                self.quarterly, datetime(2010, 5, 10, 10, 29)):
            due, remaining = self.quarterly.is_due(
                datetime(2010, 5, 10, 10, 15),
            )
            assert not due
            assert remaining == 60

    def test_daily_execution_is_due(self):
        with patch_crontab_nowfun(self.daily, datetime(2010, 5, 10, 7, 30)):
            due, remaining = self.daily.is_due(datetime(2010, 5, 9, 7, 30))
            assert due
            assert remaining == 24 * 60 * 60

    def test_daily_execution_is_not_due(self):
        with patch_crontab_nowfun(self.daily, datetime(2010, 5, 10, 10, 30)):
            due, remaining = self.daily.is_due(datetime(2010, 5, 10, 7, 30))
            assert not due
            assert remaining == 21 * 60 * 60

    def test_weekly_execution_is_due(self):
        with patch_crontab_nowfun(self.weekly, datetime(2010, 5, 6, 7, 30)):
            due, remaining = self.weekly.is_due(datetime(2010, 4, 30, 7, 30))
            assert due
            assert remaining == 7 * 24 * 60 * 60

    def test_weekly_execution_is_not_due(self):
        with patch_crontab_nowfun(self.weekly, datetime(2010, 5, 7, 10, 30)):
            due, remaining = self.weekly.is_due(datetime(2010, 5, 6, 7, 30))
            assert not due
            assert remaining == 6 * 24 * 60 * 60 - 3 * 60 * 60

    def test_monthly_execution_is_due(self):
        with patch_crontab_nowfun(self.monthly, datetime(2010, 5, 13, 7, 30)):
            due, remaining = self.monthly.is_due(datetime(2010, 4, 8, 7, 30))
            assert due
            assert remaining == 28 * 24 * 60 * 60

    def test_monthly_execution_is_not_due(self):
        with patch_crontab_nowfun(self.monthly, datetime(2010, 5, 9, 10, 30)):
            due, remaining = self.monthly.is_due(datetime(2010, 4, 8, 7, 30))
            assert not due
            assert remaining == 4 * 24 * 60 * 60 - 3 * 60 * 60

    def test_monthly_moy_execution_is_due(self):
        with patch_crontab_nowfun(
                self.monthly_moy, datetime(2014, 2, 26, 22, 0)):
            due, remaining = self.monthly_moy.is_due(
                datetime(2013, 7, 4, 10, 0),
            )
            assert due
            assert remaining == 60.0

    @pytest.mark.skip('TODO: unstable test')
    def test_monthly_moy_execution_is_not_due(self):
        with patch_crontab_nowfun(
                self.monthly_moy, datetime(2013, 6, 28, 14, 30)):
            due, remaining = self.monthly_moy.is_due(
                datetime(2013, 6, 28, 22, 14),
            )
            assert not due
            attempt = (
                time.mktime(datetime(2014, 2, 26, 22, 0).timetuple()) -
                time.mktime(datetime(2013, 6, 28, 14, 30).timetuple()) -
                60 * 60
            )
            assert remaining == attempt

    def test_monthly_moy_execution_is_due2(self):
        with patch_crontab_nowfun(
                self.monthly_moy, datetime(2014, 2, 26, 22, 0)):
            due, remaining = self.monthly_moy.is_due(
                datetime(2013, 2, 28, 10, 0),
            )
            assert due
            assert remaining == 60.0

    def test_monthly_moy_execution_is_not_due2(self):
        with patch_crontab_nowfun(
                self.monthly_moy, datetime(2014, 2, 26, 21, 0)):
            due, remaining = self.monthly_moy.is_due(
                datetime(2013, 6, 28, 22, 14),
            )
            assert not due
            attempt = 60 * 60
            assert remaining == attempt

    def test_yearly_execution_is_due(self):
        with patch_crontab_nowfun(self.yearly, datetime(2010, 3, 11, 7, 30)):
            due, remaining = self.yearly.is_due(datetime(2009, 3, 12, 7, 30))
            assert due
            assert remaining == 364 * 24 * 60 * 60

    def test_yearly_execution_is_not_due(self):
        with patch_crontab_nowfun(self.yearly, datetime(2010, 3, 7, 10, 30)):
            due, remaining = self.yearly.is_due(datetime(2009, 3, 12, 7, 30))
            assert not due
            assert remaining == 4 * 24 * 60 * 60 - 3 * 60 * 60

    def test_execution_not_due_if_task_not_run_at_last_feasible_time_outside_deadline(
            self):
        """If the crontab schedule was added after the task was due, don't
        immediately fire the task again"""
        # could have feasibly been run on 12/5 at 7:30, but wasn't.
        self.app.conf.beat_cron_starting_deadline = 3600
        last_run = datetime(2022, 12, 4, 10, 30)
        now = datetime(2022, 12, 5, 10, 30)
        expected_next_execution_time = datetime(2022, 12, 6, 7, 30)
        expected_remaining = (
            expected_next_execution_time - now).total_seconds()

        # Run the daily (7:30) crontab with the current date
        with patch_crontab_nowfun(self.daily, now):
            due, remaining = self.daily.is_due(last_run)
            assert remaining == expected_remaining
            assert not due

    def test_execution_not_due_if_task_not_run_at_last_feasible_time_no_deadline_set(
            self):
        """Same as above test except there's no deadline set, so it should be
         due"""
        last_run = datetime(2022, 12, 4, 10, 30)
        now = datetime(2022, 12, 5, 10, 30)
        expected_next_execution_time = datetime(2022, 12, 6, 7, 30)
        expected_remaining = (
            expected_next_execution_time - now).total_seconds()

        # Run the daily (7:30) crontab with the current date
        with patch_crontab_nowfun(self.daily, now):
            due, remaining = self.daily.is_due(last_run)
            assert remaining == expected_remaining
            assert due

    def test_execution_due_if_task_not_run_at_last_feasible_time_within_deadline(
            self):
        # Could have feasibly been run on 12/5 at 7:30, but wasn't. We are
        # still within a 1 hour deadline from the
        # last feasible run, so the task should still be due.
        self.app.conf.beat_cron_starting_deadline = 3600
        last_run = datetime(2022, 12, 4, 10, 30)
        now = datetime(2022, 12, 5, 8, 0)
        expected_next_execution_time = datetime(2022, 12, 6, 7, 30)
        expected_remaining = (
            expected_next_execution_time - now).total_seconds()

        # run the daily (7:30) crontab with the current date
        with patch_crontab_nowfun(self.daily, now):
            due, remaining = self.daily.is_due(last_run)
            assert remaining == expected_remaining
            assert due

    def test_execution_due_if_task_not_run_at_any_feasible_time_within_deadline(
            self):
        # Could have feasibly been run on 12/4 at 7:30, or 12/5 at 7:30,
        # but wasn't. We are still within a 1 hour
        # deadline from the last feasible run (12/5), so the task should
        # still be due.
        self.app.conf.beat_cron_starting_deadline = 3600
        last_run = datetime(2022, 12, 3, 10, 30)
        now = datetime(2022, 12, 5, 8, 0)
        expected_next_execution_time = datetime(2022, 12, 6, 7, 30)
        expected_remaining = (
            expected_next_execution_time - now).total_seconds()

        # Run the daily (7:30) crontab with the current date
        with patch_crontab_nowfun(self.daily, now):
            due, remaining = self.daily.is_due(last_run)
            assert remaining == expected_remaining
            assert due

    def test_execution_not_due_if_task_not_run_at_any_feasible_time_outside_deadline(
            self):
        """Verifies that remaining is still the time to the next
        feasible run date even though the original feasible date
        was passed over in favor of a newer one."""
        # Could have feasibly been run on 12/4 or 12/5 at 7:30,
        # but wasn't.
        self.app.conf.beat_cron_starting_deadline = 3600
        last_run = datetime(2022, 12, 3, 10, 30)
        now = datetime(2022, 12, 5, 11, 0)
        expected_next_execution_time = datetime(2022, 12, 6, 7, 30)
        expected_remaining = (
            expected_next_execution_time - now).total_seconds()

        # run the daily (7:30) crontab with the current date
        with patch_crontab_nowfun(self.daily, now):
            due, remaining = self.daily.is_due(last_run)
            assert remaining == expected_remaining
            assert not due

    def test_execution_due_if_task_not_run_at_any_feasible_time_within_deadline_on_non_uniform_schedule(self):
        # Could have feasibly been run on 12/5 9:00, 9:45, or 10:00.
        # The most recent (10:00) is 20 minutes ago, within a 30-minute
        # deadline, so the task should still be due.
        self.app.conf.beat_cron_starting_deadline = 1800
        cron = self.crontab(minute='0,45')
        last_run = datetime(2022, 12, 5, 8, 45)
        now = datetime(2022, 12, 5, 10, 20)
        expected_next_execution_time = datetime(2022, 12, 5, 10, 45)
        expected_remaining = (expected_next_execution_time - now).total_seconds()

        # Run the (:00, :45) crontab with the current date
        with patch_crontab_nowfun(cron, now):
            due, remaining = cron.is_due(last_run)
            assert remaining == expected_remaining
            assert due

    def test_execution_due_if_most_recent_feasible_run_is_exactly_on_deadline_on_non_uniform_schedule(self):
        # Could have feasibly been run on 12/5 9:00, 9:45, or 10:00.
        # The most recent (10:00) is exactly 30 minutes ago, matching the
        # deadline, so it should still be treated as due.
        self.app.conf.beat_cron_starting_deadline = 1800
        cron = self.crontab(minute='0,45')
        last_run = datetime(2022, 12, 5, 8, 45)
        now = datetime(2022, 12, 5, 10, 30)
        expected_next_execution_time = datetime(2022, 12, 5, 10, 45)
        expected_remaining = (expected_next_execution_time - now).total_seconds()
        # Run the (:00, :45) crontab with the current date
        with patch_crontab_nowfun(cron, now):
            due, remaining = cron.is_due(last_run)
            assert remaining == expected_remaining
            assert due

    def test_execution_due_if_missed_run_within_deadline_spanning_dst_start_on_non_uniform_schedule(self):
        # Could have feasibly been run on 3/11 6:00 or 3/12 0:00.
        # The most recent (3/12 0:00) is 7800 seconds ago, within the
        # 10800-second deadline even though the window spans the
        # spring-forward transition, so it should still be treated as due.
        tzname = "America/New_York"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        self.app.conf.beat_cron_starting_deadline = 10800
        cron = self.crontab(minute=0, hour='0,6')
        last_run = datetime(2023, 3, 11, 0, 0, tzinfo=tz)
        now = datetime(2023, 3, 12, 3, 10, tzinfo=tz)
        expected_next_execution_time = datetime(2023, 3, 12, 6, 0, tzinfo=tz)
        expected_remaining = (expected_next_execution_time - now).total_seconds()

        # Run the (0:00, 6:00) crontab with the current date
        with patch_crontab_nowfun(cron, now):
            due, remaining = cron.is_due(last_run)
            assert remaining == expected_remaining
            assert due

    def test_execution_not_due_if_last_run_in_future(self):
        # Should not run if the last_run hasn't happened yet.
        last_run = datetime(2022, 12, 6, 7, 30)
        now = datetime(2022, 12, 5, 10, 30)
        expected_next_execution_time = datetime(2022, 12, 7, 7, 30)
        expected_remaining = (
            expected_next_execution_time - now).total_seconds()

        # Run the daily (7:30) crontab with the current date
        with patch_crontab_nowfun(self.daily, now):
            due, remaining = self.daily.is_due(last_run)
            assert not due
            assert remaining == expected_remaining

    def test_execution_not_due_if_last_run_at_last_feasible_time(self):
        # Last feasible time is 12/5 at 7:30
        last_run = datetime(2022, 12, 5, 7, 30)
        now = datetime(2022, 12, 5, 10, 30)
        expected_next_execution_time = datetime(2022, 12, 6, 7, 30)
        expected_remaining = (
            expected_next_execution_time - now).total_seconds()

        # Run the daily (7:30) crontab with the current date
        with patch_crontab_nowfun(self.daily, now):
            due, remaining = self.daily.is_due(last_run)
            assert remaining == expected_remaining
            assert not due

    def test_execution_not_due_if_last_run_past_last_feasible_time(self):
        # Last feasible time is 12/5 at 7:30
        last_run = datetime(2022, 12, 5, 8, 30)
        now = datetime(2022, 12, 5, 10, 30)
        expected_next_execution_time = datetime(2022, 12, 6, 7, 30)
        expected_remaining = (
            expected_next_execution_time - now).total_seconds()

        # Run the daily (7:30) crontab with the current date
        with patch_crontab_nowfun(self.daily, now):
            due, remaining = self.daily.is_due(last_run)
            assert remaining == expected_remaining
            assert not due

    def test_execution_due_for_negative_utc_timezone_with_day_of_month(self):
        # UTC-8
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)

        # set day_of_month to test on _delta_to_next
        crontab = self.crontab(minute=0, day_of_month='27-31')

        # last_run_at: '2023/01/28T23:00:00-08:00'
        last_run_at = datetime(2023, 1, 28, 23, 0, tzinfo=tz)

        # now: '2023/01/29T00:00:00-08:00'
        now = datetime(2023, 1, 29, 0, 0, tzinfo=tz)

        with patch_crontab_nowfun(crontab, now):
            due, remaining = crontab.is_due(last_run_at)
            assert (due, remaining) == (True, 3600)

    def test_minute_scheduling_is_due_during_dst_start(self):
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        crontab = self.crontab()
        # Paris DST start 2024-03-31T03:00:00+02:00 + 1 minute
        last_run_at = (datetime(2024, 3, 31, 1, 0, tzinfo=ZoneInfo("UTC")) + timedelta(minutes=1)).astimezone(tz)
        now = last_run_at
        with patch_crontab_nowfun(crontab, now):
            assert crontab.is_due(last_run_at) == (False, 60)

    def test_minute_scheduling_is_due_during_dst_end(self):
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        crontab = self.crontab()
        # Paris DST end 2024-10-27T02:00:00+01:00 + 1 minute
        last_run_at = (datetime(2024, 10, 27, 1, 0, tzinfo=ZoneInfo("UTC")) + timedelta(minutes=1)).astimezone(tz)
        now = last_run_at

        with patch_crontab_nowfun(crontab, now):
            assert crontab.is_due(last_run_at) == (False, 60)

    def test_hourly_scheduling_is_due_during_dst_start(self):
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        crontab = self.crontab(minute=0)
        # Paris DST start 2024-03-31T03:00:00+02:00 minus 1 hour
        # -> 2024-03-31T01:00:00+01:00
        last_run_at = (datetime(2024, 3, 31, 1, 0, tzinfo=ZoneInfo("UTC")) + timedelta(hours=-1)).astimezone(tz)
        # Paris DST start 2024-03-31T03:00:00+02:00
        now = datetime(2024, 3, 31, 1, 0, tzinfo=ZoneInfo("UTC")).astimezone(tz)
        with patch_crontab_nowfun(crontab, now):
            assert crontab.is_due(last_run_at) == (True, 3600)

    def test_hourly_scheduling_is_due_during_dst_end(self):
        # https://github.com/celery/celery/issues/10107 regression test
        tz = ZoneInfo("Europe/Paris")
        self.app.timezone = tz
        crontab = self.crontab(minute=0)
        # Paris DST end 2024-10-27T02:00:00+01:00 minus one hour
        # -> 2024-10-27T02:00:00+01:00
        last_run_at = (datetime(2024, 10, 27, 1, 0, tzinfo=ZoneInfo("UTC")) + timedelta(hours=-1)).astimezone(tz)
        # Paris DST end 2024-10-27T02:00:00+01:00
        now = datetime(2024, 10, 27, 1, 0, tzinfo=ZoneInfo("UTC")).astimezone(tz)

        with patch_crontab_nowfun(crontab, now):
            assert crontab.is_due(last_run_at) == (True, 3600)

    def test_minute_crontab_with_negative_offset_tz_during_dst_end_is_due(self):
        # Minute-level schedule during fall-back should still be due when the
        # clock repeats the 1 AM hour.
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='*', hour='*')
        last_run_at = datetime(2024, 11, 3, 1, 59, tzinfo=tz, fold=0)  # 2024-11-03T08:59:00+00:00
        now = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=1)  # 2024-11-03T09:00:00+00:00
        print(last_run_at.isoformat(), last_run_at.astimezone(timezone.utc).isoformat())
        print(now.isoformat(), now.astimezone(timezone.utc).isoformat())
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 60)

    def test_minute_crontab_with_negative_offset_tz_during_dst_start_is_due(self):
        # Minute-level schedule during the trigger after the switch to DST (1 hour disappears).
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='*', hour='*')
        last_run_at = datetime(2024, 3, 10, 1, 59, tzinfo=tz, fold=1)  # 2024-03-10T01:59:00-08:00
        now = datetime(2024, 3, 10, 3, 0, tzinfo=tz, fold=0)  # 2024-03-10T03:00:00-07:00
        print(last_run_at.isoformat(), last_run_at.astimezone(timezone.utc).isoformat())
        print(now.isoformat(), now.astimezone(timezone.utc).isoformat())
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 60)

    def test_minute_crontab_with_positive_offset_tz_during_dst_end_is_due(self):
        # Minute-level schedule during fall-back should still be due when the
        # clock repeats the 1 AM hour.
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='*', hour='*')
        last_run_at = datetime(2024, 10, 27, 2, 59, tzinfo=tz, fold=0)  # 2024-10-27T02:59:00+02:00
        now = datetime(2024, 10, 27, 2, 0, tzinfo=tz, fold=1)  # 2024-10-27T02:00:00+01:00
        print(last_run_at.isoformat(), last_run_at.astimezone(timezone.utc).isoformat())
        print(now.isoformat(), now.astimezone(timezone.utc).isoformat())
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 60)

    def test_minute_crontab_with_positive_offset_tz_during_dst_start_is_due(self):
        # Minute-level schedule during the trigger after the switch to DST (1 hour disappears).
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='*', hour='*')
        last_run_at = datetime(2024, 3, 31, 1, 59, tzinfo=tz, fold=1)  # 2024-03-31T01:59:00+01:00
        now = datetime(2024, 3, 31, 3, 0, tzinfo=tz, fold=0)  # 2024-03-31T03:00:00+02:00
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 60)

    def test_hour_crontab_with_negative_offset_tz_during_dst_end_is_due(self):
        # local time goes 1h backward (at 2AM, it goes back to 1AM), an hourly crontab should run
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='*')
        # 2024-11-03T01:00:00-07:00 -> 2024-11-03T08:00:00Z
        last_run_at = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=0)
        # 2024-11-03T01:00:00-08:00 -> 2024-11-03T09:00:00Z
        now = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=1)
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 3600)

    def test_hour_crontab_with_negative_offset_tz_during_dst_start_is_due(self):
        # local time goes 1h forward (at 2AM, it is now 3AM, the hour in between disappears),
        # an hourly crontab should run
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='*')
        last_run_at = datetime(2024, 3, 10, 1, 0, tzinfo=tz)  # 2024-03-10T01:00:00-08:00 -> 2024-03-10T09:00:00Z
        now = datetime(2024, 3, 10, 3, 0, tzinfo=tz)          # 2024-03-10T03:00:00-07:00 -> 2024-03-10T10:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 3600)

    def test_hour_crontab_with_positive_offset_tz_during_dst_end_is_due(self):
        # local time goes 1h backward (at 3AM, it goes back to 2AM), an hourly crontab should run
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='*')
        # 2024-10-27T02:00:00+02:00 -> 2024-10-27T00:00:00Z
        last_run_at = datetime(2024, 10, 27, 2, 0, tzinfo=tz, fold=0)
        # 2024-10-27T02:00:00+01:00 -> 2024-10-27T01:00:00Z
        now = datetime(2024, 10, 27, 2, 0, tzinfo=tz, fold=1)
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 3600)

    def test_hour_crontab_with_positive_offset_tz_during_dst_start_is_due(self):
        # Hour-level schedule during the trigger after the switch to DST (1 hour disappears).
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='*')
        last_run_at = datetime(2024, 3, 31, 1, 0, tzinfo=tz)  # 2024-03-31T01:00:00+01:00 -> 2024-03-31T00:00:00Z
        now = datetime(2024, 3, 31, 3, 0, tzinfo=tz)          # 2024-03-31T03:00:00+02:00 -> 2024-03-31T01:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 3600)

    def test_daily_crontab_with_negative_offset_tz_during_dst_end_is_due(self):
        # local time goes 1h backward (at 2AM, it goes back to 1AM)
        # daily schedule should run at the correct time after transition
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='0')
        last_run_at = datetime(2024, 11, 3, 0, 0, tzinfo=tz)  # 2024-11-03T01:00:00-07:00 -> 2024-11-03T08:00:00Z
        now = datetime(2024, 11, 4, 0, 0, tzinfo=tz)          # 2024-11-03T01:00:00-08:00 -> 2024-11-03T09:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 24 * 60 * 60)

    def test_daily_crontab_with_negative_offset_tz_during_dst_start_is_due(self):
        # after the switch to DST (where 1 hour disappears),
        # daily schedule should run at the correct time after transition
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='0')
        last_run_at = datetime(2024, 3, 10, 0, 0, tzinfo=tz)  # 2024-03-10T01:00:00-08:00 -> 2024-03-10T09:00:00Z
        now = datetime(2024, 3, 11, 0, 0, tzinfo=tz)          # 2024-03-10T03:00:00-07:00 -> 2024-03-10T10:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 24 * 60 * 60)

    def test_daily_crontab_with_positive_offset_tz_during_dst_end_is_due(self):
        # local time goes 1h backward (at 3AM, it goes back to 2AM),
        # daily schedule should run at the correct time after transition
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='0')
        last_run_at = datetime(2024, 10, 27, 0, 0, tzinfo=tz)  # 2024-10-27T00:00:00+02:00 -> 2024-10-26T22:00:00Z
        now = datetime(2024, 10, 28, 0, 0, tzinfo=tz)          # 2024-10-28T00:00:00+01:00 -> 2024-10-27T23:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 24 * 60 * 60)

    def test_daily_crontab_with_positive_offset_tz_during_dst_start_is_due(self):
        # after the switch to DST (where 1 hour disappears),
        # daily schedule should run at the correct time after transition
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='0')
        last_run_at = datetime(2024, 3, 31, 0, 0, tzinfo=tz)  # 2024-03-31T00:00:00+01:00 -> 2024-03-30T23:00:00Z
        now = datetime(2024, 4, 1, 0, 0, tzinfo=tz)           # 2024-04-01T00:00:00+02:00 -> 2024-03-31T22:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (True, 24 * 60 * 60)

    def test_daily_crontab_with_negative_offset_tz_during_dst_start_is_not_due(self):
        # after the switch to DST (where 1 hour disappears),
        # daily schedule at the exact hour of the switch does not run after the switch
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='2')
        last_run_at = datetime(2024, 3, 9, 2, 0, tzinfo=tz)  # 2024-03-09T02:00:00-08:00 -> 2024-03-10T09:00:00Z
        now = datetime(2024, 3, 10, 3, 0, tzinfo=tz)         # 2024-03-10T03:00:00-07:00 -> 2024-03-10T10:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (False, 23 * 60 * 60)

    def test_daily_crontab_with_positive_offset_tz_during_dst_start_is_not_due(self):
        # after the switch to DST (where 1 hour disappears),
        # daily schedule at the exact hour of the switch does not run after the switch
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='0', hour='2')
        last_run_at = datetime(2024, 3, 30, 2, 0, tzinfo=tz)  # 2024-03-30T02:00:00+01:00 -> 2024-03-30T01:00:00Z
        now = datetime(2024, 3, 31, 3, 0, tzinfo=tz)          # 2024-03-31T03:00:00+02:00 -> 2024-03-31T01:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (False, 23 * 60 * 60)

    def test_daily_crontab_with_negative_offset_tz_during_dst_start_is_really_not_due(self):
        # after the switch to DST (where 1 hour disappears),
        # daily schedule at the exact hour of the switch does not run after the switch
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='30', hour='2')
        last_run_at = datetime(2024, 3, 9, 2, 30, tzinfo=tz)  # 2024-03-09T02:30:00-08:00 -> 2024-03-09T09:00:00Z
        now = datetime(2024, 3, 10, 3, 30, tzinfo=tz)         # 2024-03-10T03:00:00-07:00 -> 2024-03-10T10:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (False, 23 * 60 * 60)

    def test_daily_crontab_with_positive_offset_tz_during_dst_start_is_really_not_due(self):
        # after the switch to DST (where 1 hour disappears),
        # daily schedule at the exact hour of the switch does not run after the switch
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='30', hour='2')
        last_run_at = datetime(2024, 3, 30, 2, 30, tzinfo=tz)  # 2024-03-30T02:30:00+01:00 -> 2024-03-30T01:30:00Z
        now = datetime(2024, 3, 31, 3, 30, tzinfo=tz)          # 2024-03-31T03:30:00+02:00 -> 2024-03-31T01:30:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (False, 23 * 60 * 60)

    def test_daily_crontab_with_positive_offset_tz_during_dst_start_is_due_next_day(self):
        # after the switch to DST (where 1 hour disappears),
        # daily schedule at the exact hour of the switch does not run after the switch
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute='30', hour='2')
        last_run_at = datetime(2024, 3, 31, 1, 0, tzinfo=tz)  # 2024-03-30T01:00:00+01:00 -> 2024-03-31T00:00:00Z
        now = datetime(2024, 3, 31, 1, 0, tzinfo=tz)          # 2024-03-31T01:00:00+01:00 -> 2024-03-31T00:00:00Z
        ct.nowfun = lambda: now

        is_due, rem = ct.is_due(last_run_at)
        assert (is_due, rem) == (False, 24 * 60 * 60 + 30 * 60)

    def test_hourly_crontab_during_dst_fall_back(self):
        # Test for #10107: hourly crontab skips execution during fall-back
        # DST transition when the same local hour occurs twice.
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute=0, hour='*')
        # Fall-back Nov 3, 2024 America/Los_Angeles:
        #   1:00 AM PDT (UTC-7, fold=0) = 08:00 UTC
        #   1:00 AM PST (UTC-8, fold=1) = 09:00 UTC
        last_run_at = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=0)  # 1 AM PDT
        now = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=1)          # 1 AM PST
        ct.nowfun = lambda: now

        remaining = ct.remaining_estimate(last_run_at)
        # One real hour has passed (8 UTC → 9 UTC), task should be due
        assert remaining.total_seconds() <= 0

    def test_hourly_crontab_during_dst_fall_back_is_due(self):
        # Same as above but testing via is_due()
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute=0, hour='*')

        last_run_at = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=0)  # 1 AM PDT
        now = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=1)          # 1 AM PST
        ct.nowfun = lambda: now

        is_due, next_time = ct.is_due(last_run_at)
        assert is_due

    def test_daily_crontab_during_dst_fall_back_not_due(self):
        # Daily at 1:00 AM should NOT fire twice on the same calendar day
        # when the hour 1:00 occurs twice during fall-back.
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute=0, hour=1)

        # Fall-back Nov 3, 2024:
        #   1:00 AM PDT (UTC-7, fold=0) = 08:00 UTC
        #   1:00 AM PST (UTC-8, fold=1) = 09:00 UTC
        last_run_at = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=0)  # 1 AM PDT
        now = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=1)          # 1 AM PST
        ct.nowfun = lambda: now

        remaining = ct.remaining_estimate(last_run_at)
        # Task ran once at 1:00 AM PDT; it should next run the following day,
        # so it must not be considered due again at 1:00 AM PST.
        assert remaining.total_seconds() == 0

    def test_hourly_crontab_during_dst_spring_forward_is_due(self):
        # Hourly schedule across the spring-forward gap should still be due.
        # In America/Los_Angeles on 2024-03-10, clocks jump from 2 AM to 3 AM.
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute=0, hour='*')

        last_run_at = datetime(2024, 3, 10, 1, 0, tzinfo=tz)
        now = datetime(2024, 3, 10, 3, 0, tzinfo=tz)
        ct.nowfun = lambda: now

        is_due, next_time = ct.is_due(last_run_at)
        assert is_due

    def test_hourly_crontab_during_dst_fall_back_europe_london_is_due(self):
        # Hourly schedule should fire during Europe/London fall-back.
        # Oct 27, 2024: clocks go back from 2 AM BST to 1 AM GMT.
        tzname = "Europe/London"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute=0, hour='*')

        last_run_at = datetime(2024, 10, 27, 1, 0, tzinfo=tz, fold=0)
        now = datetime(2024, 10, 27, 1, 0, tzinfo=tz, fold=1)
        ct.nowfun = lambda: now

        is_due, next_time = ct.is_due(last_run_at)
        assert is_due

    def test_hourly_crontab_no_dst_transition_normal_behavior(self):
        # Regression: hourly schedule in a DST-aware timezone should work
        # normally when no DST transition is happening.
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute=0, hour='*')

        last_run_at = datetime(2024, 7, 15, 14, 0, tzinfo=tz)
        now = datetime(2024, 7, 15, 15, 0, tzinfo=tz)
        ct.nowfun = lambda: now

        is_due, next_time = ct.is_due(last_run_at)
        assert is_due

    def test_hourly_crontab_dst_fall_back_stale_last_run_not_triggered(self):
        # Guard: UTC proximity check.  If last_run_at is from much earlier
        # (> 2 hours in UTC), the DST fall-back shortcut should NOT apply.
        # The task should still be due via the normal scheduling path.
        tzname = "America/Los_Angeles"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute=0, hour='*')

        # Last run at 10 PM on Nov 2 (well before fall-back).
        last_run_at = datetime(2024, 11, 2, 22, 0, tzinfo=tz)
        # Now is 1 AM PST on Nov 3 (post fall-back, fold=1).
        now = datetime(2024, 11, 3, 1, 0, tzinfo=tz, fold=1)
        ct.nowfun = lambda: now

        is_due, next_time = ct.is_due(last_run_at)
        # Should be due via normal path (many hours elapsed), not DST shortcut
        assert is_due

    def test_hourly_crontab_dst_fall_back_australia(self):
        # Southern hemisphere: Australia/Sydney falls back on first Sunday
        # of April.  2024-04-07: clocks go from 3 AM AEDT to 2 AM AEST.
        # The 2 AM hour occurs twice.
        tzname = "Australia/Sydney"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        ct = self.crontab(minute=0, hour='*')

        # First 2 AM (AEDT, fold=0) and second 2 AM (AEST, fold=1).
        last_run_at = datetime(2024, 4, 7, 2, 0, tzinfo=tz, fold=0)
        now = datetime(2024, 4, 7, 2, 0, tzinfo=tz, fold=1)
        ct.nowfun = lambda: now

        is_due, next_time = ct.is_due(last_run_at)
        assert is_due

    def test_hour_after_dst_end(self):
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        crontab = self.crontab(minute=10)

        # Set last_run_at just before DST end
        last_run_at = datetime(2017, 10, 29, 0, 10, tzinfo=timezone.utc).astimezone(tz)
        now = datetime(2017, 10, 29, 1, 0, tzinfo=timezone.utc).astimezone(tz)
        crontab.nowfun = lambda: now

        assert crontab.remaining_estimate(last_run_at) == timedelta(minutes=10)

    def test_hour_after_dst_start(self):
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        crontab = self.crontab(minute=10)

        # Set last_run_at Before DST start
        last_run_at = datetime(2017, 3, 26, 0, 10, tzinfo=timezone.utc).astimezone(tz)
        # Set now after DST start
        now = datetime(2017, 3, 26, 1, 0, tzinfo=timezone.utc).astimezone(tz)
        crontab.nowfun = lambda: now
        assert crontab.remaining_estimate(last_run_at) == timedelta(minutes=10)

    def test_end_of_day_after_dst_start(self):
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        crontab = self.crontab(hour=23, minute=59)

        # start of the day
        last_run_at = datetime(2017, 3, 26, 0, 0, tzinfo=tz)
        now = datetime(2017, 3, 26, 0, 0, tzinfo=tz)
        crontab.nowfun = lambda: now

        # 23h minus 1 minute
        assert crontab.remaining_estimate(last_run_at).total_seconds() == 23 * 60 * 60 - 60

    def test_end_of_day_after_dst_end(self):
        tzname = "Europe/Paris"
        self.app.timezone = tzname
        tz = ZoneInfo(tzname)
        crontab = self.crontab(hour=23, minute=59)

        # start of the day
        last_run_at = datetime(2017, 10, 29, 0, 0, tzinfo=tz)
        now = datetime(2017, 10, 29, 0, 0, tzinfo=tz)
        crontab.nowfun = lambda: now

        # 25h minus 1 minute
        assert crontab.remaining_estimate(last_run_at).total_seconds() == 25 * 60 * 60 - 60
