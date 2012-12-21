from __future__ import absolute_import
from __future__ import with_statement

from datetime import datetime, timedelta

from mock import Mock

from celery.exceptions import ImproperlyConfigured
from celery.utils import timeutils
from celery.utils.timeutils import timezone
from celery.tests.utils import Case


class test_timeutils(Case):

    def test_delta_resolution(self):
        D = timeutils.delta_resolution

        dt = datetime(2010, 3, 30, 11, 50, 58, 41065)
        deltamap = ((timedelta(days=2), datetime(2010, 3, 30, 0, 0)),
                    (timedelta(hours=2), datetime(2010, 3, 30, 11, 0)),
                    (timedelta(minutes=2), datetime(2010, 3, 30, 11, 50)),
                    (timedelta(seconds=2), dt))
        for delta, shoulda in deltamap:
            self.assertEqual(D(dt, delta), shoulda)

    def test_timedelta_seconds(self):
        deltamap = ((timedelta(seconds=1), 1),
                    (timedelta(seconds=27), 27),
                    (timedelta(minutes=3), 3 * 60),
                    (timedelta(hours=4), 4 * 60 * 60),
                    (timedelta(days=3), 3 * 86400))
        for delta, seconds in deltamap:
            self.assertEqual(timeutils.timedelta_seconds(delta), seconds)

    def test_timedelta_seconds_returns_0_on_negative_time(self):
        delta = timedelta(days=-2)
        self.assertEqual(timeutils.timedelta_seconds(delta), 0)

    def test_humanize_seconds(self):
        t = ((4 * 60 * 60 * 24, '4.00 days'),
             (1 * 60 * 60 * 24, '1.00 day'),
             (4 * 60 * 60, '4.00 hours'),
             (1 * 60 * 60, '1.00 hour'),
             (4 * 60, '4.00 minutes'),
             (1 * 60, '1.00 minute'),
             (4, '4.00 seconds'),
             (1, '1.00 second'),
             (4.3567631221, '4.36 seconds'),
             (0, 'now'))

        for seconds, human in t:
            self.assertEqual(timeutils.humanize_seconds(seconds), human)

        self.assertEqual(timeutils.humanize_seconds(4, prefix='about '),
                         'about 4.00 seconds')

    def test_maybe_iso8601_datetime(self):
        now = datetime.now()
        self.assertIs(timeutils.maybe_iso8601(now), now)

    def test_maybe_timedelta(self):
        D = timeutils.maybe_timedelta

        for i in (30, 30.6):
            self.assertEqual(D(i), timedelta(seconds=i))

        self.assertEqual(D(timedelta(days=2)), timedelta(days=2))

    def test_remaining_relative(self):
        timeutils.remaining(datetime.utcnow(), timedelta(hours=1),
                            relative=True)


class test_timezone(Case):

    def test_get_timezone_with_pytz(self):
        prev, timeutils.pytz = timeutils.pytz, Mock()
        try:
            self.assertTrue(timezone.get_timezone('UTC'))
        finally:
            timeutils.pytz = prev

    def test_get_timezone_without_pytz(self):
        prev, timeutils.pytz = timeutils.pytz, None
        try:
            self.assertTrue(timezone.get_timezone('UTC'))
            with self.assertRaises(ImproperlyConfigured):
                timezone.get_timezone('Europe/Oslo')
        finally:
            timeutils.pytz = prev
