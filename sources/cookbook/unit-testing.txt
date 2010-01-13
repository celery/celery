================
 Unit Testing
================

Testing with Django
-------------------

The problem that you'll first run in to when trying to write a test that runs a
task is that Django's testrunner doesn't use the same database that your celery
daemon is using. If you're using the database backend, this means that your
tombstones won't show up in your test database and you won't be able to check
on your tasks to get the return value or check the status.

There are two ways to get around this. You can either take advantage of
``CELERY_ALWAYS_EAGER = True`` to skip the daemon, or you can avoid testing
anything that needs to check the status or result of a task.

Using a custom testrunner to test with celery
---------------------------------------------

If you're going the ``CELERY_ALWAYS_EAGER`` route, which is probably better than
just never testing some parts of your app, a custom Django testrunner does the
trick. Celery provides a simple testrunner, but it's easy enough to roll your
own if you have other things that need to be done.
http://docs.djangoproject.com/en/dev/topics/testing/#defining-a-test-runner

For this example, we'll use the ``celery.contrib.test_runner`` to test the
``add`` task from the :doc:`User Guide: Tasks<../userguide/tasks>` examples.

To enable the testrunner, set the following settings:

.. code-block:: python

    TEST_RUNNER = 'celery.contrib.test_runner.run_tests'


Then we can write our actually test in a ``tests.py`` somewhere:

.. code-block:: python

    from django.test import TestCase
    from myapp.tasks import add

    class AddTestCase(TestCase):

        def testNoError(self):
            """Test that the ``add`` task runs with no errors,
            and returns the correct result."""
            result = add.delay(8, 8)

            self.assertEquals(result.get(), 16)
            self.assertTrue(result.successful())


This test assumes that you put your example ``add`` task in ``maypp.tasks``
so of course adjust the import for wherever you actually put the class.
