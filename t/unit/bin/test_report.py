# -*- coding: utf-8 -*-
"""Tests for ``celery report`` command."""
from __future__ import absolute_import, unicode_literals

from case import Mock, call, patch
from celery.bin.celery import report
from celery.five import WhateverIO


class test_report:
    """Test report command class."""

    def test_run(self):
        out = WhateverIO()
        with patch(
            'celery.loaders.base.BaseLoader.import_default_modules'
        ) as import_default_modules:
            with patch(
                'celery.app.base.Celery.bugreport'
            ) as bugreport:
                # Method call order mock obj
                mco = Mock()
                mco.attach_mock(import_default_modules, 'idm')
                mco.attach_mock(bugreport, 'br')
                a = report(app=self.app, stdout=out)
                a.run()
                calls = [call.idm(), call.br()]
                mco.assert_has_calls(calls)
