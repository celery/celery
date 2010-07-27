=============================
 Running celeryd as a daemon
=============================

Celery does not daemonize itself, please use one of the following
daemonization tools.

.. contents::
    :local:


start-stop-daemon (Debian/Ubuntu/++)
====================================

See the `contrib/debian/init.d/`_ directory in the celery distribution, this
directory contains init scripts for celeryd and celerybeat.

These scripts are configured in ``/etc/default/celeryd``.

.. _`contrib/debian/init.d/`:
    http://github.com/ask/celery/tree/master/contrib/debian/

Init script: celeryd
--------------------

:Usage: ``/etc/init.d/celeryd {start|stop|force-reload|restart|try-restart|status}``
:Configuration file: /etc/default/celeryd

To configure celeryd you probably need to at least tell it where to chdir
when it starts (to find your celeryconfig).

Example configuration
~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for a Python project.

``/etc/default/celeryd``::

    # Where to chdir at start.
    CELERYD_CHDIR="/opt/Myproject/"

    # Extra arguments to celeryd
    CELERYD_OPTS="--time-limit 300"

    # Name of the celery config module.#
    CELERY_CONFIG_MODULE="celeryconfig"

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for those using ``django-celery``::

    # Where the Django project is.
    CELERYD_CHDIR="/opt/Project/"

    # Name of the projects settings module.
    DJANGO_SETTINGS_MODULE="settings"

    # Path to celeryd
    CELERYD="/opt/Project/manage.py celeryd"

Available options
~~~~~~~~~~~~~~~~~~

* CELERYD_OPTS
    Additional arguments to celeryd, see ``celeryd --help`` for a list.

* CELERYD_CHDIR
    Path to chdir at start. Default is to stay in the current directory.

* CELERYD_PID_FILE
    Full path to the pidfile. Default is /var/run/celeryd.pid.

* CELERYD_LOG_FILE
    Full path to the celeryd logfile. Default is /var/log/celeryd.log

* CELERYD_LOG_LEVEL
    Log level to use for celeryd. Default is INFO.

* CELERYD
    Path to the celeryd program. Default is ``celeryd``.
    You can point this to an virtualenv, or even use manage.py for django.

* CELERYD_USER
    User to run celeryd as. Default is current user.

* CELERYD_GROUP
    Group to run celeryd as. Default is current user.

Init script: celerybeat
-----------------------
:Usage: ``/etc/init.d/celerybeat {start|stop|force-reload|restart|try-restart|status}``
:Configuration file: /etc/default/celerybeat or /etc/default/celeryd


Example configuration
~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for a Python project:

``/etc/default/celeryd``::

    # Where to chdir at start.
    CELERYD_CHDIR="/opt/Myproject/"

    # Extra arguments to celeryd
    CELERYD_OPTS="--time-limit 300"

    # Extra arguments to celerybeat
    CELERYBEAT_OPTS="--schedule=/var/run/celerybeat-schedule"

    # Name of the celery config module.#
    CELERY_CONFIG_MODULE="celeryconfig"

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for those using ``django-celery``::

    # Where the Django project is.
    CELERYD_CHDIR="/opt/Project/"

    # Name of the projects settings module.
    DJANGO_SETTINGS_MODULE="settings"

    # Path to celeryd
    CELERYD="/opt/Project/manage.py celeryd"

    # Path to celerybeat
    CELERYBEAT="/opt/Project/manage.py celerybeat"

    # Extra arguments to celerybeat
    CELERYBEAT_OPTS="--schedule=/var/run/celerybeat-schedule"

Available options
~~~~~~~~~~~~~~~~~

* CELERYBEAT_OPTS
    Additional arguments to celerybeat, see ``celerybeat --help`` for a
    list.

* CELERYBEAT_PIDFILE
    Full path to the pidfile. Default is /var/run/celeryd.pid.

* CELERYBEAT_LOGFILE
    Full path to the celeryd logfile. Default is /var/log/celeryd.log

* CELERYBEAT_LOG_LEVEL
    Log level to use for celeryd. Default is INFO.

* CELERYBEAT
    Path to the celeryd program. Default is ``celeryd``.
    You can point this to an virtualenv, or even use manage.py for django.

* CELERYBEAT_USER
    User to run celeryd as. Default is current user.

* CELERYBEAT_GROUP
    Group to run celeryd as. Default is current user.

Troubleshooting
---------------

If you can't get the init scripts to work, you should try running
them in *verbose mode*::

    $ sh -x /etc/init.d/celeryd start

This can reveal hints as to why the service won't start.

Also you will see the commands generated, so you can try to run the celeryd
command manually to read the resulting error output.

For example my ``sh -x`` output does this::

    ++ start-stop-daemon --start --chdir /opt/Opal/release/opal --quiet \
        --oknodo --background --make-pidfile --pidfile /var/run/celeryd.pid \
        --exec /opt/Opal/release/opal/manage.py celeryd -- --time-limit=300 \
        -f /var/log/celeryd.log -l INFO

Run the celeryd command after ``--exec`` (without the ``--``) to show the
actual resulting output::

    $ /opt/Opal/release/opal/manage.py celeryd --time-limit=300 \
        -f /var/log/celeryd.log -l INFO


`supervisord`_
==============

* `contrib/supervisord/`_

.. _`contrib/supervisord/`:
    http://github.com/ask/celery/tree/master/contrib/supervisord/
.. _`supervisord`: http://supervisord.org/

launchd (OS X)
==============

* `contrib/mac/`_

.. _`contrib/mac/`:
    http://github.com/ask/celery/tree/master/contrib/mac/
