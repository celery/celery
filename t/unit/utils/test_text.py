import pytest

from celery.utils.text import (abbr, abbrtask, ensure_newlines, indent, pretty, remove_repeating_from_task,
                               simple_format, truncate)

RANDTEXT = """\
The quick brown
fox jumps
over the
lazy dog\
"""

RANDTEXT_RES = """\
    The quick brown
    fox jumps
    over the
    lazy dog\
"""

QUEUES = {
    'queue1': {
        'exchange': 'exchange1',
        'exchange_type': 'type1',
        'routing_key': 'bind1',
    },
    'queue2': {
        'exchange': 'exchange2',
        'exchange_type': 'type2',
        'routing_key': 'bind2',
    },
}


QUEUE_FORMAT1 = '.> queue1           exchange=exchange1(type1) key=bind1'
QUEUE_FORMAT2 = '.> queue2           exchange=exchange2(type2) key=bind2'


class test_Info:

    def test_textindent(self):
        assert indent(RANDTEXT, 4) == RANDTEXT_RES

    def test_format_queues(self, app):
        app.amqp.queues = app.amqp.Queues(QUEUES)
        assert (sorted(app.amqp.queues.format().split('\n')) ==
                sorted([QUEUE_FORMAT1, QUEUE_FORMAT2]))

    def test_ensure_newlines(self):
        assert len(ensure_newlines('foo\nbar\nbaz\n').splitlines()) == 3
        assert len(ensure_newlines('foo\nbar').splitlines()) == 2


@pytest.mark.parametrize('s,maxsize,expected', [
    ('ABCDEFGHI', 3, 'ABC...'),
    ('ABCDEFGHI', 10, 'ABCDEFGHI'),

])
def test_truncate_text(s, maxsize, expected):
    assert truncate(s, maxsize) == expected


@pytest.mark.parametrize('args,expected', [
    ((None, 3), '???'),
    (('ABCDEFGHI', 6), 'ABC...'),
    (('ABCDEFGHI', 20), 'ABCDEFGHI'),
    (('ABCDEFGHI', 6, None), 'ABCDEF'),
])
def test_abbr(args, expected):
    assert abbr(*args) == expected


@pytest.mark.parametrize('s,maxsize,expected', [
    (None, 3, '???'),
    ('feeds.tasks.refresh', 10, '[.]refresh'),
    ('feeds.tasks.refresh', 30, 'feeds.tasks.refresh'),
])
def test_abbrtask(s, maxsize, expected):
    assert abbrtask(s, maxsize) == expected


def test_pretty():
    assert pretty(('a', 'b', 'c'))


@pytest.mark.parametrize('value,expected', [
    ('%%', '%'),
    ('%%n', '%n'),
    ('%%x', '%x'),
    ('%%%n', '%worker'),
    ('%%%%n', '%%n'),
    ('%n-%%-%n', 'worker-%-worker'),
])
def test_simple_format_percent_escape(value, expected):
    assert simple_format(value, {'n': 'worker'}) == expected


def test_simple_format_unknown_key():
    with pytest.raises(ValueError, match='Unknown format %x'):
        simple_format('%x', {})


@pytest.mark.parametrize('task_name,s,expected', [
    ('tasks.add',
     'tasks.add(2, 2), tasks.mul(3), tasks.div(4)',
     'tasks.add(2, 2), mul(3), div(4)'),
    ('x.tasks.add',
     'x.tasks.add(2, 2) | x.tasks.add(4) | x.tasks.mul(8)',
     'x.tasks.add(2, 2) | add(4) | mul(8)'),
    # Only whole module prefixes are removed, not the module name appearing
    # at the end of another module name.
    ('app.tasks.add',
     'app.tasks.add(2, 2) | webapp.tasks.mul(3)',
     'app.tasks.add(2, 2) | webapp.tasks.mul(3)'),
    ('app.tasks.add',
     'app.tasks.add(2, 2) | my-app.tasks.mul(3)',
     'app.tasks.add(2, 2) | my-app.tasks.mul(3)'),
    # A task name without a module must not strip every dot from the string.
    ('add',
     'add(1.5) | proj.tasks.mul(2.5)',
     'add(1.5) | proj.tasks.mul(2.5)'),
])
def test_remove_repeating_from_task(task_name, s, expected):
    assert remove_repeating_from_task(task_name, s) == expected
