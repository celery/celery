from __future__ import absolute_import, unicode_literals

import os
import pprint

import pytest


def _get_extras_reqs_from(name):
    try:
        with open(os.path.join('requirements', name)) as fh:
            lines = fh.readlines()
    except OSError:
        pytest.skip('requirements dir missing, not running from dist?')
    else:
        return {
            line.split()[1] for line in lines
            if line.startswith('-r extras/')
        }


def _get_all_extras():
    return set(
        os.path.join('extras', f)
        for f in os.listdir('requirements/extras/')
    )


def test_all_reqs_enabled_in_tests():
    ci_default = _get_extras_reqs_from('test-ci-default.txt')
    ci_base = _get_extras_reqs_from('test-ci-base.txt')

    defined = ci_default | ci_base
    all_extras = _get_all_extras()
    diff = all_extras - defined
    print('Missing CI reqs:\n{0}'.format(pprint.pformat(diff)))
    assert not diff
