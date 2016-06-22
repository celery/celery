from __future__ import absolute_import, unicode_literals

from celery.utils import debug

from celery.tests.case import Case, Mock, patch


class test_on_blocking(Case):

    @patch('inspect.getframeinfo')
    def test_on_blocking(self, getframeinfo):
        frame = Mock(name='frame')
        with self.assertRaises(RuntimeError):
            debug._on_blocking(1, frame)
            getframeinfo.assert_called_with(frame)


class test_blockdetection(Case):

    @patch('celery.utils.debug.signals')
    def test_context(self, signals):
        with debug.blockdetection(10):
            signals.arm_alarm.assert_called_with(10)
            signals.__setitem__.assert_called_with('ALRM', debug._on_blocking)
        signals.__setitem__.assert_called_with('ALRM', signals['ALRM'])
        signals.reset_alarm.assert_called_with()


class test_sample_mem(Case):

    @patch('celery.utils.debug.mem_rss')
    def test_sample_mem(self, mem_rss):
        prev, debug._mem_sample = debug._mem_sample, []
        try:
            debug.sample_mem()
            self.assertIs(debug._mem_sample[0], mem_rss())
        finally:
            debug._mem_sample = prev


class test_sample(Case):

    def test_sample(self):
        x = list(range(100))
        self.assertEqual(
            list(debug.sample(x, 10)),
            [0, 10, 20, 30, 40, 50, 60, 70, 80, 90],
        )
        x = list(range(91))
        self.assertEqual(
            list(debug.sample(x, 10)),
            [0, 9, 18, 27, 36, 45, 54, 63, 72, 81],
        )


class test_hfloat(Case):

    def test_hfloat(self):
        self.assertEqual(str(debug.hfloat(10, 5)), '10')
        self.assertEqual(str(debug.hfloat(10.45645234234, 5)), '10.456')


class test_humanbytes(Case):

    def test_humanbytes(self):
        self.assertEqual(debug.humanbytes(2 ** 20), '1MB')
        self.assertEqual(debug.humanbytes(4 * 2 ** 20), '4MB')
        self.assertEqual(debug.humanbytes(2 ** 16), '64KB')
        self.assertEqual(debug.humanbytes(2 ** 16), '64KB')
        self.assertEqual(debug.humanbytes(2 ** 8), '256b')


class test_mem_rss(Case):

    @patch('celery.utils.debug.ps')
    @patch('celery.utils.debug.humanbytes')
    def test_mem_rss(self, humanbytes, ps):
        ret = debug.mem_rss()
        ps.assert_called_with()
        ps().memory_info.assert_called_with()
        humanbytes.assert_called_with(ps().memory_info().rss)
        self.assertIs(ret, humanbytes())
        ps.return_value = None
        self.assertIsNone(debug.mem_rss())


class test_ps(Case):

    @patch('celery.utils.debug.Process')
    @patch('os.getpid')
    def test_ps(self, getpid, Process):
        prev, debug._process = debug._process, None
        try:
            debug.ps()
            Process.assert_called_with(getpid())
            self.assertIs(debug._process, Process())
        finally:
            debug._process = prev
