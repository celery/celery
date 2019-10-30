from __future__ import absolute_import, unicode_literals

import pytest
from case import Mock, patch

from celery.bin.base import Error
from celery.bin.control import _RemoteControl, control, inspect, status
from celery.five import WhateverIO


class test_RemoteControl:

    def test_call_interface(self):
        with pytest.raises(NotImplementedError):
            _RemoteControl(app=self.app).call()


class test_inspect:

    def test_usage(self):
        assert inspect(app=self.app).usage('foo')

    def test_command_info(self):
        i = inspect(app=self.app)
        assert i.get_command_info(
            'ping', help=True, color=i.colored.red, app=self.app,
        )

    def test_list_commands_color(self):
        i = inspect(app=self.app)
        assert i.list_commands(help=True, color=i.colored.red, app=self.app)
        assert i.list_commands(help=False, color=None, app=self.app)

    def test_epilog(self):
        assert inspect(app=self.app).epilog

    def test_do_call_method_sql_transport_type(self):
        self.app.connection = Mock()
        conn = self.app.connection.return_value = Mock(name='Connection')
        conn.transport.driver_type = 'sql'
        i = inspect(app=self.app)
        with pytest.raises(i.Error):
            i.do_call_method(['ping'])

    def test_say_directions(self):
        i = inspect(self.app)
        i.out = Mock()
        i.quiet = True
        i.say_chat('<-', 'hello out')
        i.out.assert_not_called()

        i.say_chat('->', 'hello in')
        i.out.assert_called()

        i.quiet = False
        i.out.reset_mock()
        i.say_chat('<-', 'hello out', 'body')
        i.out.assert_called()

    @patch('celery.app.control.Control.inspect')
    def test_run(self, real):
        out = WhateverIO()
        i = inspect(app=self.app, stdout=out)
        with pytest.raises(Error):
            i.run()
        with pytest.raises(Error):
            i.run('help')
        with pytest.raises(Error):
            i.run('xyzzybaz')

        i.run('ping')
        real.assert_called()
        i.run('ping', destination='foo,bar')
        assert real.call_args[1]['destination'], ['foo' == 'bar']
        assert real.call_args[1]['timeout'] == 0.2
        callback = real.call_args[1]['callback']

        callback({'foo': {'ok': 'pong'}})
        assert 'OK' in out.getvalue()

        with patch('celery.bin.control.dumps') as dumps:
            i.run('ping', json=True)
            dumps.assert_called()

        instance = real.return_value = Mock()
        instance._request.return_value = None
        with pytest.raises(Error):
            i.run('ping')

        out.seek(0)
        out.truncate()
        i.quiet = True
        i.say_chat('<-', 'hello')
        assert not out.getvalue()


class test_control:

    def control(self, patch_call, *args, **kwargs):
        kwargs.setdefault('app', Mock(name='app'))
        c = control(*args, **kwargs)
        if patch_call:
            c.call = Mock(name='control.call')
        return c

    def test_call(self):
        i = self.control(False)
        i.call('foo', arguments={'kw': 2})
        i.app.control.broadcast.assert_called_with(
            'foo', arguments={'kw': 2}, reply=True)


class test_status:

    @patch('celery.bin.control.inspect')
    def test_run(self, inspect_):
        out, err = WhateverIO(), WhateverIO()
        ins = inspect_.return_value = Mock()
        ins.run.return_value = []
        s = status(self.app, stdout=out, stderr=err)
        with pytest.raises(Error):
            s.run()

        ins.run.return_value = ['a', 'b', 'c']
        s.run()
        assert '3 nodes online' in out.getvalue()
        s.run(quiet=True)
