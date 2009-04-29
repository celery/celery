import unittest

from celery import task
from celery import registry

# Task run functions can't be closures/lambdas, as they're pickled.
def return_True(self, **kwargs):
    return True

class TestCeleryTasks(unittest.TestCase):

    def createTaskCls(self, cls_name, task_name=None):
        attrs = {}
        if task_name:
            attrs["name"] = task_name
        cls = type(cls_name, (task.Task, ), attrs)
        cls.run = return_True
        return cls

    def assertNextTaskDataEquals(self, consumer, task_id, task_name,
            **kwargs):
        next_task = consumer.fetch()
        task_data = consumer.decoder(next_task.body)
        self.assertEquals(task_data["celeryID"], task_id)
        self.assertEquals(task_data["celeryTASK"], task_name)
        for arg_name, arg_value in kwargs.items():
            self.assertEquals(task_data.get(arg_name), arg_value)

    def test_regular_task(self):
    
            T1 = self.createTaskCls("T1", "c.unittest.t.t1")
            self.assertTrue(isinstance(T1(), T1))
            self.assertTrue(T1().run())
            self.assertTrue(callable(T1()),
                    "Task class is callable()")
            self.assertTrue(T1()(),
                    "Task class runs run() when called")
            
            # task without name raises NotImplementedError
            T2 = self.createTaskCls("T2")
            self.assertRaises(NotImplementedError, T2)

            registry.tasks.register(T1)
            t1 = T1()
            consumer = t1.get_consumer()
            consumer.discard_all()
            self.assertTrue(consumer.fetch() is None)

            # Without arguments.
            tid = t1.delay()
            self.assertNextTaskDataEquals(consumer, tid, t1.name)

            # With arguments.
            tid2 = task.delay_task(t1.name, name="George Constanza")
            self.assertNextTaskDataEquals(consumer, tid2, t1.name,
                    name="George Constanza")

            self.assertRaises(registry.tasks.NotRegistered, task.delay_task,
                    "some.task.that.should.never.exist.X.X.X.X.X")

            # Discarding all tasks.
            task.discard_all()
            tid3 = task.delay_task(t1.name)
            self.assertEquals(task.discard_all(), 1)
            self.assertTrue(consumer.fetch() is None)

            self.assertFalse(task.is_done(tid))
            task.mark_as_done(tid, result=None)
            self.assertTrue(task.is_done(tid))
