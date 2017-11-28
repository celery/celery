# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import pytest
from case import skip

from celery.five import text_t
from celery.utils import term
from celery.utils.term import colored, fg


@skip.if_win32()
class test_colored:

    @pytest.fixture(autouse=True)
    def preserve_encoding(self, patching):
        patching('sys.getdefaultencoding', 'utf-8')

    @pytest.mark.parametrize('name,color', [
        ('black', term.BLACK),
        ('red', term.RED),
        ('green', term.GREEN),
        ('yellow', term.YELLOW),
        ('blue', term.BLUE),
        ('magenta', term.MAGENTA),
        ('cyan', term.CYAN),
        ('white', term.WHITE),
    ])
    def test_colors(self, name, color):
        assert fg(30 + color) in str(colored().names[name]('foo'))

    @pytest.mark.parametrize('name', [
        'bold', 'underline', 'blink', 'reverse', 'bright',
        'ired', 'igreen', 'iyellow', 'iblue', 'imagenta',
        'icyan', 'iwhite', 'reset',
    ])
    def test_modifiers(self, name):
        assert str(getattr(colored(), name)('f'))

    def test_unicode(self):
        assert text_t(colored().green('∂bar'))
        assert colored().red('éefoo') + colored().green('∂bar')
        assert colored().red('foo').no_color() == 'foo'

    def test_repr(self):
        assert repr(colored().blue('åfoo'))
        assert "''" in repr(colored())

    def test_more_unicode(self):
        c = colored()
        s = c.red('foo', c.blue('bar'), c.green('baz'))
        assert s.no_color()
        c._fold_no_color(s, 'øfoo')
        c._fold_no_color('fooå', s)

        c = colored().red('åfoo')
        assert c._add(c, 'baræ') == '\x1b[1;31m\xe5foo\x1b[0mbar\xe6'

        c2 = colored().blue('ƒƒz')
        c3 = c._add(c, c2)
        assert c3 == '\x1b[1;31m\xe5foo\x1b[0m\x1b[1;34m\u0192\u0192z\x1b[0m'
