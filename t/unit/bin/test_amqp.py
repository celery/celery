from __future__ import absolute_import, unicode_literals

import pytest
from case import Mock, patch

from celery.bin.amqp import AMQPAdmin, AMQShell, amqp, dump_message, main
from celery.five import WhateverIO


class test_AMQShell:

    def setup(self):
        self.fh = WhateverIO()
        self.adm = self.create_adm()
        self.shell = AMQShell(connect=self.adm.connect, out=self.fh)

    def create_adm(self, *args, **kwargs):
        return AMQPAdmin(app=self.app, out=self.fh, *args, **kwargs)

    def test_queue_declare(self):
        self.shell.onecmd('queue.declare foo')
        assert 'ok' in self.fh.getvalue()

    def test_missing_command(self):
        self.shell.onecmd('foo foo')
        assert 'unknown syntax' in self.fh.getvalue()

    def RV(self):
        raise Exception(self.fh.getvalue())

    def test_spec_format_response(self):
        spec = self.shell.amqp['exchange.declare']
        assert spec.format_response(None) == 'ok.'
        assert spec.format_response('NO') == 'NO'

    def test_missing_namespace(self):
        self.shell.onecmd('ns.cmd arg')
        assert 'unknown syntax' in self.fh.getvalue()

    def test_help(self):
        self.shell.onecmd('help')
        assert 'Example:' in self.fh.getvalue()

    def test_help_command(self):
        self.shell.onecmd('help queue.declare')
        assert 'passive:no' in self.fh.getvalue()

    def test_help_unknown_command(self):
        self.shell.onecmd('help foo.baz')
        assert 'unknown syntax' in self.fh.getvalue()

    def test_onecmd_error(self):
        self.shell.dispatch = Mock()
        self.shell.dispatch.side_effect = MemoryError()
        self.shell.say = Mock()
        assert not self.shell.needs_reconnect
        self.shell.onecmd('hello')
        self.shell.say.assert_called()
        assert self.shell.needs_reconnect

    def test_exit(self):
        with pytest.raises(SystemExit):
            self.shell.onecmd('exit')
        assert "don't leave!" in self.fh.getvalue()

    def test_note_silent(self):
        self.shell.silent = True
        self.shell.note('foo bar')
        assert 'foo bar' not in self.fh.getvalue()

    def test_reconnect(self):
        self.shell.onecmd('queue.declare foo')
        self.shell.needs_reconnect = True
        self.shell.onecmd('queue.delete foo')

    def test_completenames(self):
        assert self.shell.completenames('queue.dec') == ['queue.declare']
        assert (sorted(self.shell.completenames('declare')) ==
                sorted(['queue.declare', 'exchange.declare']))

    def test_empty_line(self):
        self.shell.emptyline = Mock()
        self.shell.default = Mock()
        self.shell.onecmd('')
        self.shell.emptyline.assert_called_with()
        self.shell.onecmd('foo')
        self.shell.default.assert_called_with('foo')

    def test_respond(self):
        self.shell.respond({'foo': 'bar'})
        assert 'foo' in self.fh.getvalue()

    def test_prompt(self):
        assert self.shell.prompt

    def test_no_returns(self):
        self.shell.onecmd('queue.declare foo')
        self.shell.onecmd('exchange.declare bar direct yes')
        self.shell.onecmd('queue.bind foo bar baz')
        self.shell.onecmd('basic.ack 1')

    def test_dump_message(self):
        m = Mock()
        m.body = 'the quick brown fox'
        m.properties = {'a': 1}
        m.delivery_info = {'exchange': 'bar'}
        assert dump_message(m)

    def test_dump_message_no_message(self):
        assert 'No messages in queue' in dump_message(None)

    def test_note(self):
        self.adm.silent = True
        self.adm.note('FOO')
        assert 'FOO' not in self.fh.getvalue()

    def test_run(self):
        a = self.create_adm('queue.declare', 'foo')
        a.run()
        assert 'ok' in self.fh.getvalue()

    def test_run_loop(self):
        a = self.create_adm()
        a.Shell = Mock()
        shell = a.Shell.return_value = Mock()
        shell.cmdloop = Mock()
        a.run()
        shell.cmdloop.assert_called_with()

        shell.cmdloop.side_effect = KeyboardInterrupt()
        a.run()
        assert 'bibi' in self.fh.getvalue()

    @patch('celery.bin.amqp.amqp')
    def test_main(self, Command):
        c = Command.return_value = Mock()
        main()
        c.execute_from_commandline.assert_called_with()

    @patch('celery.bin.amqp.AMQPAdmin')
    def test_command(self, cls):
        x = amqp(app=self.app)
        x.run()
        assert cls.call_args[1]['app'] is self.app
