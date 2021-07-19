import os

import pytest

try:
    from sphinx.application import Sphinx  # noqa: F401
    from sphinx_testing import TestApp
    sphinx_installed = True
except ImportError:
    sphinx_installed = False


SRCDIR = os.path.join(os.path.dirname(__file__), 'proj')


@pytest.mark.skipif(
    sphinx_installed is False,
    reason='Sphinx is not installed'
)
def test_sphinx():
    app = TestApp(srcdir=SRCDIR, confdir=SRCDIR)
    app.build()
    contents = open(os.path.join(app.outdir, 'contents.html'),
                    encoding='utf-8').read()
    assert 'This is a sample Task' in contents
    assert 'This is a sample Shared Task' in contents
    assert (
        'This task is in a different module!'
        not in contents
    )
