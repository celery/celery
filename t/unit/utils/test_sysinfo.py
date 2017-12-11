<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from case import skip

from celery.utils.sysinfo import df, load_average


@skip.unless_symbol('os.getloadavg')
def test_load_average(patching):
    getloadavg = patching('os.getloadavg')
    getloadavg.return_value = 0.54736328125, 0.6357421875, 0.69921875
    l = load_average()
    assert l
    assert l == (0.55, 0.64, 0.7)


@skip.unless_symbol('posix.statvfs_result')
def test_df():
    x = df('/')
    assert x.total_blocks
    assert x.available
    assert x.capacity
    assert x.stat
