=============================
 Running celeryd as a daemon
=============================

Celery does not daemonize itself, please use one of the following
daemonization tools.


start-stop-daemon
=================

* `contrib/debian/init.d/`_

.. _`contrib/debian/init.d/`:
    http://github.com/ask/celery/tree/master/contrib/debian/

`supervisord`_
==============

* `contrib/supervisord/`_

.. _`contrib/supervisord/`:
    http://github.com/ask/celery/tree/master/contrib/supervisord/
.. _`supervisord`: http://supervisord.org/


launchd (OS X)
==============

* `contrib/mac`_

.. _`contrib/mac/`:
    http://github.com/ask/celery/tree/master/contrib/mac/
