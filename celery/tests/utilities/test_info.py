from __future__ import absolute_import

from celery import Celery
from celery.utils.text import indent
from celery.tests.utils import Case

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


QUEUE_FORMAT1 = """.> queue1:      exchange:exchange1(type1) binding:bind1"""
QUEUE_FORMAT2 = """.> queue2:      exchange:exchange2(type2) binding:bind2"""


class test_Info(Case):

    def test_textindent(self):
        self.assertEqual(indent(RANDTEXT, 4), RANDTEXT_RES)

    def test_format_queues(self):
        celery = Celery(set_as_current=False)
        celery.amqp.queues = celery.amqp.Queues(QUEUES)
        self.assertEqual(sorted(celery.amqp.queues.format().split('\n')),
                         sorted([QUEUE_FORMAT1, QUEUE_FORMAT2]))
