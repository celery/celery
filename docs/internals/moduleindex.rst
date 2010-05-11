==============
 Module Index
==============

Worker
======

celery.worker
-------------

celery.worker.job
-----------------

celery.worker.pool
------------------

celery.worker.listener
----------------------

celery.worker.controllers
-------------------------

celery.worker.scheduler
-----------------------

celery.worker.buckets
---------------------

celery.worker.heartbeat
-----------------------

celery.worker.revoke
--------------------

celery.worker.control
---------------------

* celery.worker.registry

* celery.worker.builtins


Tasks
=====

celery.decorators
-----------------

celery.registry
---------------

celery.task
-----------

celery.task.base
----------------

celery.task.http
----------------

celery.task.rest
----------------

Backward compatible interface to :mod:`celery.task.http`.
Will be deprecated in future versions.

celery.task.control
-------------------

celery.task.builtins
--------------------

Execution
=========

celery.execute
--------------

celery.execute.trace
--------------------

celery.result
-------------

celery.states
-------------

celery.signals

Messaging
=========

celery.messaging
----------------

Django-specific
===============

celery.models
-------------

celery.managers
---------------

celery.views
------------

celery.urls
-----------

celery.management

Result backends
===============

celery.backends
---------------

celery.backends.base
--------------------

celery.backends.amqp
--------------------

celery.backends.database
------------------------

Loaders
=======

celery.loaders
--------------

Loader autodetection, and working with the currently
selected loader.

celery.loaders.base - Loader base classes
-----------------------------------------

celery.loaders.default - The default loader
-------------------------------------------

celery.loaders.djangoapp - The Django loader
--------------------------------------------

CeleryBeat
==========

celery.beat
-----------

Events
======

celery.events
-------------

Logging
=======

celery.log
----------

celery.utils.patch
------------------

Configuration
=============

celery.conf
-----------

Miscellaneous
=============

celery.datastructures
---------------------

celery.exceptions
-----------------

celery.platform
---------------

celery.utils
------------

celery.utils.info
-----------------

celery.utils.compat
-------------------
