from __future__ import absolute_import, unicode_literals

import pytest
from case import Mock
from kombu.five import WhateverIO

from celery.bin.base import Error
from celery.bin.list import list_


class test_list:

    def test_list_bindings_no_support(self):
        l = list_(app=self.app, stderr=WhateverIO())
        management = Mock()
        management.get_bindings.side_effect = NotImplementedError()
        with pytest.raises(Error):
            l.list_bindings(management)

    def test_run(self):
        l = list_(app=self.app, stderr=WhateverIO())
        l.run('bindings')

        with pytest.raises(Error):
            l.run(None)

        with pytest.raises(Error):
            l.run('foo')
