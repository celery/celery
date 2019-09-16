from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock, patch
from celery.bin.migrate import migrate
from celery.five import WhateverIO


class test_migrate:

    @patch('celery.contrib.migrate.migrate_tasks')
    def test_run(self, migrate_tasks):
        out = WhateverIO()
        m = migrate(app=self.app, stdout=out, stderr=WhateverIO())
        with pytest.raises(TypeError):
            m.run()
        migrate_tasks.assert_not_called()

        m.run('memory://foo', 'memory://bar')
        migrate_tasks.assert_called()

        state = Mock()
        state.count = 10
        state.strtotal = 30
        m.on_migrate_task(state, {'task': 'tasks.add', 'id': 'ID'}, None)
        assert '10/30' in out.getvalue()
