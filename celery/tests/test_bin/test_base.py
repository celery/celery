import unittest2 as unittest

from celery.bin.base import Command
from celery.datastructures import AttributeDict


class Object(object):
    pass


class MockCommand(Command):

    def parse_options(self, prog_name, arguments):
        options = Object()
        options.foo = "bar"
        options.prog_name = prog_name
        return options, (10, 20, 30)

    def run(self, *args, **kwargs):
        return args, kwargs


class test_Command(unittest.TestCase):

    def test_get_options(self):
        cmd = Command()
        cmd.option_list = (1, 2, 3)
        self.assertTupleEqual(cmd.get_options(), (1, 2, 3))

    def test_run_interface(self):
        self.assertRaises(NotImplementedError, Command().run)

    def test_execute_from_commandline(self):
        cmd = MockCommand()
        args1, kwargs1 = cmd.execute_from_commandline()     # sys.argv
        self.assertTupleEqual(args1, (10, 20, 30))
        self.assertDictContainsSubset({"foo": "bar"}, kwargs1)
        self.assertTrue(kwargs1.get("prog_name"))
        args2, kwargs2 = cmd.execute_from_commandline(["foo"])   # pass list
        self.assertTupleEqual(args2, (10, 20, 30))
        self.assertDictContainsSubset({"foo": "bar", "prog_name": "foo"},
                                      kwargs2)
