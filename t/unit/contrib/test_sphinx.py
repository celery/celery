from __future__ import absolute_import, unicode_literals

import pkg_resources
import pytest

try:
    sphinx_build = pkg_resources.load_entry_point(
        'sphinx', 'console_scripts', 'sphinx-build')
except pkg_resources.DistributionNotFound:
    sphinx_build = None


@pytest.mark.skipif(sphinx_build is None, reason='Sphinx is not installed')
def test_sphinx(tmpdir):
    srcdir = pkg_resources.resource_filename(__name__, 'proj')
    sphinx_build([srcdir, str(tmpdir)])
    with open(tmpdir / 'contents.html', 'r') as f:
        contents = f.read()
    assert 'This task has a docstring!' in contents
    assert 'This task is in a different module!' not in contents
