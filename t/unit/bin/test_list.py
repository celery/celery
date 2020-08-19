import pytest
from case import Mock

from celery.bin.base import Error
from celery.bin.list import list_
from celery.utils.text import WhateverIO


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
