import celery.contrib.conflator
from celery.contrib.conflator import Conflator
from celery.tests.case import AppCase, depends_on_current_app
from celery.tests.tasks.test_tasks import TasksCase
from celery.utils.log import get_logger

logger = get_logger(__name__)


class test_ConflatingTask(TasksCase):

    task_count = 0

    class ConflatingCounter(Conflator):

        conflation_key = 'a'
        task_count = 0

        def __init__(self, parent):
            self.parent = parent
            super(Conflator, self).__init__()

        def run(self):
            logger.debug("incrementing task_count")
            self.parent.task_count += 1

    class DyingConflater(Conflator):

        conflation_key = 'a'

        def run(self):
            raise Exception("boom!")

    def setup(self):
        celery.contrib.conflator.cache = None
        self.task_count = 0
        self.app.conf.CELERY_CACHE_BACKEND = 'memory://'

    def _test_conflator_multiple_tasks(self, total):
        """ Schedule N tasks """

        # FIXME: It would be better if this could spin up a worker and
        # run through the actually scheduled tasks on the queue
        to_run = []
        for i in range(total):
            t = self.ConflatingCounter(self)
            if t.delay():
                to_run.append(t)

        for t in to_run:
            t()

        self.assertEqual(1, self.task_count)

    @depends_on_current_app
    def test_conflator_three_at_once(self):
        self._test_conflator_multiple_tasks(3)

    @depends_on_current_app
    def test_conflator_task_after(self):
        self._test_conflator_multiple_tasks(2)
        # Next task should execute as it's scheduled after the first
        # two have executed
        t = self.ConflatingCounter(self)
        if t.delay():
            t()
        self.assertEqual(2, self.task_count)

    @depends_on_current_app
    def test_raising_conflator(self):
        first_counter = self.ConflatingCounter(self)
        if first_counter.delay():
            first_counter()

        raiser = self.DyingConflater()
        if raiser.delay():
            self.assertRaises(Exception, raiser)
        else:
            self.fail("DyingConflator should have been scheduled")

        second_counter = self.ConflatingCounter(self)
        if second_counter.delay():
            second_counter()

        self.assertEquals(2, self.task_count)

    @depends_on_current_app
    def test_common_conflation_key(self):
        first_counter = self.ConflatingCounter(self)
        first_counter.delay()

        raiser = self.DyingConflater()
        if raiser.delay():
            self.fail("DyingConflator should not have been scheduled")

        first_counter()
        
        second_counter = self.ConflatingCounter(self)
        if second_counter.delay():
            second_counter()

        self.assertEquals(2, self.task_count)

    @depends_on_current_app
    def test_rehydrating_conflator(self):
        first_counter = self.ConflatingCounter(self)

        second_counter = self.ConflatingCounter(self)
        second_counter.delay()
        second_counter()
        self.assertEquals(1, self.task_count)
