from __future__ import absolute_import, unicode_literals

import pytest
import time

from contextlib import contextmanager
from datetime import datetime, timedelta
from pickle import dumps, loads

from case import Case, Mock, skip

from celery.five import items
from celery.schedules import (
    ParseException, crontab, crontab_parser, schedule, solar,
)

assertions = Case('__init__')


@contextmanager
def patch_crontab_nowfun(cls, retval):
    prev_nowfun = cls.nowfun
    cls.nowfun = lambda: retval
    try:
        yield
    finally:
        cls.nowfun = prev_nowfun


@skip.unless_module('ephem')
class test_solar:

    def setup(self):
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
        assert self.s.is_due(datetime.utcnow()).is_due

    def test_is_due__not_due(self):
        self.s.remaining_estimate = Mock(name='rem')
        self.s.remaining_estimate.return_value = timedelta(hours=10)
        assert not self.s.is_due(datetime.utcnow()).is_due

    def test_remaining_estimate(self):
        self.s.cal = Mock(name='cal')
        self.s.cal.next_rising().datetime.return_value = datetime.utcnow()
        self.s.remaining_estimate(datetime.utcnow())

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


class test_crontab_parser:

    def crontab(self, *args, **kwargs):
        return crontab(*args, **dict(kwargs, app=self.app))

    def test_crontab_reduce(self):
        assert loads(dumps(self.crontab('*')))

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


class test_crontab_remaining_estimate:

    def crontab(self, *args, **kwargs):
        return crontab(*args, **dict(kwargs, app=self.app))

    def next_ocurrance(self, crontab, now):
        crontab.nowfun = lambda: now
        return now + crontab.remaining_estimate(now)

    def test_next_minute(self):
        next = self.next_ocurrance(
            self.crontab(), datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 11, 14, 31)

    def test_not_next_minute(self):
        next = self.next_ocurrance(
            self.crontab(), datetime(2010, 9, 11, 14, 59, 15),
        )
        assert next == datetime(2010, 9, 11, 15, 0)

    def test_this_hour(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42]), datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 11, 14, 42)

    def test_not_this_hour(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 10, 15]),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 11, 15, 5)

    def test_today(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], hour=[12, 17]),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 11, 17, 5)

    def test_not_today(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], hour=[12]),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 12, 12, 5)

    def test_weekday(self):
        next = self.next_ocurrance(
            self.crontab(minute=30, hour=14, day_of_week='sat'),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 18, 14, 30)

    def test_not_weekday(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='mon-fri'),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 13, 0, 5)

    def test_monthday(self):
        next = self.next_ocurrance(
            self.crontab(minute=30, hour=14, day_of_month=18),
            datetime(2010, 9, 11, 14, 30, 15),
        )
        assert next == datetime(2010, 9, 18, 14, 30)

    def test_not_monthday(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_month=29),
            datetime(2010, 1, 22, 14, 30, 15),
        )
        assert next == datetime(2010, 1, 29, 0, 5)

    def test_weekday_monthday(self):
        next = self.next_ocurrance(
            self.crontab(minute=30, hour=14,
                         day_of_week='mon', day_of_month=18),
            datetime(2010, 1, 18, 14, 30, 15),
        )
        assert next == datetime(2010, 10, 18, 14, 30)

    def test_monthday_not_weekday(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='sat', day_of_month=29),
            datetime(2010, 1, 29, 0, 5, 15),
        )
        assert next == datetime(2010, 5, 29, 0, 5)

    def test_weekday_not_monthday(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='mon', day_of_month=18),
            datetime(2010, 1, 11, 0, 5, 15),
        )
        assert next == datetime(2010, 1, 18, 0, 5)

    def test_not_weekday_not_monthday(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='mon', day_of_month=18),
            datetime(2010, 1, 10, 0, 5, 15),
        )
        assert next == datetime(2010, 1, 18, 0, 5)

    def test_leapday(self):
        next = self.next_ocurrance(
            self.crontab(minute=30, hour=14, day_of_month=29),
            datetime(2012, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2012, 2, 29, 14, 30)

    def test_not_leapday(self):
        next = self.next_ocurrance(
            self.crontab(minute=30, hour=14, day_of_month=29),
            datetime(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2010, 3, 29, 14, 30)

    def test_weekmonthdayyear(self):
        next = self.next_ocurrance(
            self.crontab(minute=30, hour=14, day_of_week='fri',
                         day_of_month=29, month_of_year=1),
            datetime(2010, 1, 22, 14, 30, 15),
        )
        assert next == datetime(2010, 1, 29, 14, 30)

    def test_monthdayyear_not_week(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='wed,thu',
                         day_of_month=29, month_of_year='1,4,7'),
            datetime(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2010, 4, 29, 0, 5)

    def test_weekdaymonthyear_not_monthday(self):
        next = self.next_ocurrance(
            self.crontab(minute=30, hour=14, day_of_week='fri',
                         day_of_month=29, month_of_year='1-10'),
            datetime(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2010, 10, 29, 14, 30)

    def test_weekmonthday_not_monthyear(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='fri',
                         day_of_month=29, month_of_year='2-10'),
            datetime(2010, 1, 29, 14, 30, 15),
        )
        assert next == datetime(2010, 10, 29, 0, 5)

    def test_weekday_not_monthdayyear(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='mon',
                         day_of_month=18, month_of_year='2-10'),
            datetime(2010, 1, 11, 0, 5, 15),
        )
        assert next == datetime(2010, 10, 18, 0, 5)

    def test_monthday_not_weekdaymonthyear(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='mon',
                         day_of_month=29, month_of_year='2-4'),
            datetime(2010, 1, 29, 0, 5, 15),
        )
        assert next == datetime(2010, 3, 29, 0, 5)

    def test_monthyear_not_weekmonthday(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='mon',
                         day_of_month=29, month_of_year='2-4'),
            datetime(2010, 2, 28, 0, 5, 15),
        )
        assert next == datetime(2010, 3, 29, 0, 5)

    def test_not_weekmonthdayyear(self):
        next = self.next_ocurrance(
            self.crontab(minute=[5, 42], day_of_week='fri,sat',
                         day_of_month=29, month_of_year='2-10'),
            datetime(2010, 1, 28, 14, 30, 15),
        )
        assert next == datetime(2010, 5, 29, 0, 5)

    def test_invalid_specification(self):
        # *** WARNING ***
        # This test triggers an infinite loop in case of a regression
        with pytest.raises(RuntimeError):
            self.next_ocurrance(
                self.crontab(day_of_month=31, month_of_year=4),
                datetime(2010, 1, 28, 14, 30, 15),
            )

    def test_leapyear(self):
        next = self.next_ocurrance(
            self.crontab(minute=30, hour=14, day_of_month=29, month_of_year=2),
            datetime(2012, 2, 29, 14, 30),
        )
        assert next == datetime(2016, 2, 29, 14, 30)


class test_crontab_is_due:

    def setup(self):
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
        ('2,4,6', {2, 4, 6}),
        ('*/2', {1, 3, 5, 7, 9, 11}),
        ('2-12/2', {2, 4, 6, 8, 10, 12}),
    ])
    def test_crontab_spec_moy_formats(self, month_of_year, expected):
        c = self.crontab(month_of_year=month_of_year)
        assert c.month_of_year == expected

    @pytest.mark.parametrize('month_of_year', [0, '0-5', 13, '12,13'])
    def test_crontab_spec_invalid_moy(self, month_of_year):
        with pytest.raises(ValueError):
            self.crontab(month_of_year=month_of_year)

    def seconds_almost_equal(self, a, b, precision):
        for index, skew in enumerate((+1, -1, 0)):
            try:
                assertions.assertAlmostEqual(a, b + skew, precision)
            except Exception as exc:
                # AssertionError != builtins.AssertionError in py.test
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
            for field, value in items(d1._fields()):
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

    @skip.todo('unstable test')
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
