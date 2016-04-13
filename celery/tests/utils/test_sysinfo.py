from __future__ import absolute_import, unicode_literals

from celery.utils.sysinfo import load_average, df

from celery.tests.case import Case, patch, skip


@skip.unless_symbol('os.getloadavg')
class test_load_average(Case):

    def test_avg(self):
        with patch('os.getloadavg') as getloadavg:
            getloadavg.return_value = 0.54736328125, 0.6357421875, 0.69921875
            l = load_average()
            self.assertTrue(l)
            self.assertEqual(l, (0.55, 0.64, 0.7))


@skip.unless_symbol('posix.statvfs_result')
class test_df(Case):

    def test_df(self):
        x = df('/')
        self.assertTrue(x.total_blocks)
        self.assertTrue(x.available)
        self.assertTrue(x.capacity)
        self.assertTrue(x.stat)
