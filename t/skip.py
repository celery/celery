import sys

import pytest

if_pypy = pytest.mark.skipif(getattr(sys, 'pypy_version_info', None), reason='PyPy not supported.')
if_win32 = pytest.mark.skipif(sys.platform.startswith('win32'), reason='Does not work on Windows')
