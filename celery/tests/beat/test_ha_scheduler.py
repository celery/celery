from kombu import uuid


from celery.app.defaults import flatten

from celery.app.defaults import NAMESPACES
from celery.app.utils import Settings
from celery.beat.scheduler.failover.base_failover import BaseFailoverStrategy
from celery.beat.scheduler.ha_scheduler import HAScheduler
from celery.tests.app.test_beat import always_due
from celery.tests.case import AppCase, Mock

locked = False

task_sent = []


class FakeFailoverStrategy(BaseFailoverStrategy):
    connection_type = "fake"
    lock_owner = False

    def is_master(self):
        global locked
        if not locked:
            locked = True
            self.lock_owner = True
            return locked
        else:
            return self.lock_owner


class HAMockedScheduler(HAScheduler):
    def __init__(self, *args, **kwargs):
        super(HAMockedScheduler, self).__init__(*args, **kwargs)

    def send_task(self, name=None, args=None, kwargs=None, **options):
        task_sent.append({'name': name,
                          'args': args,
                          'kwargs': kwargs,
                          'options': options})
        return self.app.AsyncResult(uuid())


class test_HAScheduler(AppCase):
    def setup(self):
        self.expected_task = "test_task"
        self.app = Mock(main='source', tasks={}, conf=Settings({}, [{"BROKER_URL": "fake://blah_blah//"}, dict((key, value.default) for key, value in flatten(NAMESPACES))]),
                        connection=Mock(SimpleQueue=Mock()))

        self.first_scheduler = HAMockedScheduler(self.app)
        self.second_scheduler = HAMockedScheduler(self.app)

        schedule_dict = {
            'test_ticks':
                {
                    'task': self.expected_task,
                    'schedule': always_due
                }
        }

        self.first_scheduler.update_from_dict(schedule_dict)
        self.second_scheduler.update_from_dict(schedule_dict)
        global task_sent
        global locked
        task_sent = []
        locked = False

    def test_by_one_scheduler(self):
        self.assertEqual(0, len(task_sent))
        self.first_scheduler.tick()
        self.assertEqual(1, len(task_sent))
        self.assertEqual(self.expected_task, task_sent[0]["name"])
        self.assertEqual((), task_sent[0]["args"])
        self.assertDictEqual({}, task_sent[0]["kwargs"])

    def test_by_two_scheduler(self):
        """
        This is a simulation of running multiple beat instances with HAScheduler and guaranties one of them always submitting task into queue
        Two schedulers ticks but one of them will be sending task and it is seen that, there will be 1 task in queue instead of two.
        """

        self.assertEqual(0, len(task_sent))
        self.first_scheduler.tick()
        self.second_scheduler.tick()
        self.assertEqual(1, len(task_sent))
        self.assertEqual(self.expected_task, task_sent[0]["name"])
        self.assertEqual((), task_sent[0]["args"])
        self.assertDictEqual({}, task_sent[0]["kwargs"])


    def test_by_two_scheduler_with_multiple_task_running(self):
        """
        This is a simulation of running multiple beat instances with HAScheduler and guaranties one of them always submitting task into queue
        Two schedulers ticks but one of them will be sending task and it is seen that, there will be 1 task in queue instead of two.
        """
        self.first_scheduler.schedule = {}
        self.second_scheduler.schedule = {}

        task_size = 10
        schedule_dict = dict(('test_ticks%s' % i, {'task': "%s.%s" % (self.expected_task, i), 'schedule': always_due}) for i in range(task_size))

        self.first_scheduler.update_from_dict(schedule_dict)
        self.second_scheduler.update_from_dict(schedule_dict)

        self.assertEqual(0, len(task_sent))

        for i in range(task_size):
            self.first_scheduler.tick()
            self.second_scheduler.tick()
        self.assertEqual(task_size, len(task_sent))