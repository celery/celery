==================
 Monitoring Guide
==================

.. contents::
    :local:

Events
======

Describe events


Snapshots
---------

Describe snapshots


Custom Camera
~~~~~~~~~~~~~

.. code-block:: python

    from pprint import pformat

    from celery.events.snapshot import Polaroid

    class DumpCam(Polaroid):

        def shutter(self, state):
            if not state.event_count:
                # No new events since last snapshot.
                return
            print("Workers: %s" % (pformat(state.workers, indent=4), ))
            print("Tasks: %s" % (pformat(state.tasks, indent=4), ))
            print("Total: %s events, %s tasks" % (
                state.event_count, state.task_count))

Now you can use this cam with ``celeryev`` by specifying
it with the ``-c`` option::

    $ celeryev -c myapp.DumpCam --frequency=2.0

Or you can use it programatically like this::

    from celery.events import EventReceiver
    from celery.messaging import establish_connection
    from celery.events.state import State
    from myapp import DumpCam

    def main():
        state = State()
        with establish_connection() as connection:
            recv = EventReceiver(connection, handlers={"*": state.event})
            with DumpCam(state, freq=1.0):
                recv.capture(limit=None, timeout=None)

    if __name__ == "__main__":
        main()




Tools
=====

celerymon
=========

Describe celerymon

celeryev
========

Describe celeryev

RabbitMQ
========

Describe rabbitmq tools. rabbitmqctl, Alice, etc...

Django Admin
============

Describe the snapshot camera django-celery ships with.

Munin
=====

Maintain a list of related munin plugins
