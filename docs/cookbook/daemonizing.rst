=============================
 Running celeryd as a daemon
=============================

Celery is not a daemon by itself. When run in production it should use
an appropriate daemonizing tool on the platform.

For example start-up scripts see ``contrib/debian/init.d`` for using
``start-stop-daemon`` on Debian/Ubuntu, or ``contrib/mac/org.celeryq.*`` for using
``launchd`` on Mac OS X.

.. _`supervisord`: http://supervisord.org/

