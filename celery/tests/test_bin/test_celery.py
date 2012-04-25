from __future__ import absolute_import
from __future__ import with_statement

from anyjson import dumps
from datetime import datetime
from mock import Mock, patch

from celery import task
from celery.platforms import EX_FAILURE, EX_USAGE, EX_OK
from celery.bin.celery import (
    Command,
    Error,
    worker,
    list_,
    apply,
    purge,
    result,
    inspect,
    status,
    migrate,
    shell,
    help,
    report,
    CeleryCommand,
    determine_exit_status,
    main,
)

from celery.tests.utils import AppCase, WhateverIO


@task
def add(x, y):
    return x + y


class test_Command(AppCase):

    def test_Error_repr(self):
        x = Error("something happened")
        self.assertIsNotNone(x.status)
        self.assertTrue(x.reason)
        self.assertTrue(str(x))

    def setup(self):
        self.out = WhateverIO()
        self.err = WhateverIO()
        self.cmd = Command(self.app, stdout=self.out, stderr=self.err)

    def test_show_help(self):
        self.cmd.run_from_argv = Mock()
        self.assertEqual(self.cmd.show_help("foo"), EX_USAGE)
        self.cmd.run_from_argv.assert_called_with(
                self.cmd.prog_name, ["foo", "--help"]
        )

    def test_error(self):
        self.cmd.out = Mock()
        self.cmd.error("FOO")
        self.assertTrue(self.cmd.out.called)

    def test_out(self):
        f = Mock()
        self.cmd.out("foo", f)
        f.write.assert_called_with("foo\n")
        self.cmd.out("foo\n", f)

    def test_call(self):
        self.cmd.run = Mock()
        self.cmd.run.return_value = None
        self.assertEqual(self.cmd(), EX_OK)

        self.cmd.run.side_effect = Error("error", EX_FAILURE)
        self.assertEqual(self.cmd(), EX_FAILURE)

    def test_run_from_argv(self):
        with self.assertRaises(NotImplementedError):
            self.cmd.run_from_argv("prog", ["foo", "bar"])
        self.assertEqual(self.cmd.prog_name, "prog")

    def test_prettify_list(self):
        self.assertEqual(self.cmd.prettify([])[1], "- empty -")
        self.assertIn("bar", self.cmd.prettify(["foo", "bar"])[1])

    def test_prettify_dict(self):
        self.assertIn("OK",
            str(self.cmd.prettify({"ok": "the quick brown fox"})[0]))
        self.assertIn("ERROR",
            str(self.cmd.prettify({"error": "the quick brown fox"})[0]))

    def test_prettify(self):
        self.assertIn("OK", str(self.cmd.prettify("the quick brown")))
        self.assertIn("OK", str(self.cmd.prettify(object())))
        self.assertIn("OK", str(self.cmd.prettify({"foo": "bar"})))


class test_Delegate(AppCase):

    def test_get_options(self):
        self.assertTrue(worker().get_options())

    def test_run(self):
        w = worker()
        w.target.run = Mock()
        w.run()
        w.target.run.assert_called_with()


class test_list(AppCase):

    def test_list_bindings_no_support(self):
        l = list_(app=self.app, stderr=WhateverIO())
        management = Mock()
        management.get_bindings.side_effect = NotImplementedError()
        with self.assertRaises(Error):
            l.list_bindings(management)

    def test_run(self):
        l = list_(app=self.app, stderr=WhateverIO())
        l.run("bindings")

        with self.assertRaises(Error):
            l.run(None)

        with self.assertRaises(Error):
            l.run("foo")


class test_apply(AppCase):

    @patch("celery.app.base.Celery.send_task")
    def test_run(self, send_task):
        a = apply(app=self.app, stderr=WhateverIO(), stdout=WhateverIO())
        a.run("tasks.add")
        self.assertTrue(send_task.called)

        a.run("tasks.add",
              args=dumps([4, 4]),
              kwargs=dumps({"x": 2, "y": 2}))
        self.assertEqual(send_task.call_args[1]["args"], [4, 4])
        self.assertEqual(send_task.call_args[1]["kwargs"], {"x": 2, "y": 2})

        a.run("tasks.add", expires=10, countdown=10)
        self.assertEqual(send_task.call_args[1]["expires"], 10)
        self.assertEqual(send_task.call_args[1]["countdown"], 10)

        now = datetime.now()
        iso = now.isoformat()
        a.run("tasks.add", expires=iso)
        self.assertEqual(send_task.call_args[1]["expires"], now)
        with self.assertRaises(ValueError):
            a.run("tasks.add", expires="foobaribazibar")


class test_purge(AppCase):

    @patch("celery.app.control.Control.discard_all")
    def test_run(self, discard_all):
        out = WhateverIO()
        a = purge(app=self.app, stdout=out)
        discard_all.return_value = 0
        a.run()
        self.assertIn("No messages purged", out.getvalue())

        discard_all.return_value = 100
        a.run()
        self.assertIn("100 messages", out.getvalue())


class test_result(AppCase):

    @patch("celery.result.AsyncResult.get")
    def test_run(self, get):
        out = WhateverIO()
        r = result(app=self.app, stdout=out)
        get.return_value = "Jerry"
        r.run("id")
        self.assertIn("Jerry", out.getvalue())

        get.return_value = "Elaine"
        r.run("id", task=add.name)
        self.assertIn("Elaine", out.getvalue())


class test_status(AppCase):

    @patch("celery.bin.celery.inspect")
    def test_run(self, inspect_):
        out, err = WhateverIO(), WhateverIO()
        ins = inspect_.return_value = Mock()
        ins.run.return_value = []
        s = status(self.app, stdout=out, stderr=err)
        with self.assertRaises(Error):
            s.run()

        ins.run.return_value = ["a", "b", "c"]
        s.run()
        self.assertIn("3 nodes online", out.getvalue())
        s.run(quiet=True)


class test_migrate(AppCase):

    @patch("celery.contrib.migrate.migrate_tasks")
    def test_run(self, migrate_tasks):
        out = WhateverIO()
        m = migrate(app=self.app, stdout=out, stderr=WhateverIO())
        with self.assertRaises(SystemExit):
            m.run()
        self.assertFalse(migrate_tasks.called)

        m.run("memory://foo", "memory://bar")
        self.assertTrue(migrate_tasks.called)

        state = Mock()
        state.count = 10
        state.strtotal = 30
        m.on_migrate_task(state, {"task": "tasks.add", "id": "ID"}, None)
        self.assertIn("10/30", out.getvalue())


class test_report(AppCase):

    def test_run(self):
        out = WhateverIO()
        r = report(app=self.app, stdout=out)
        self.assertEqual(r.run(), EX_OK)
        self.assertTrue(out.getvalue())


class test_help(AppCase):

    def test_run(self):
        out = WhateverIO()
        h = help(app=self.app, stdout=out)
        h.parser = Mock()
        self.assertEqual(h.run(), EX_USAGE)
        self.assertTrue(out.getvalue())
        self.assertTrue(h.usage("help"))
        h.parser.print_help.assert_called_with()
