.. _daemonizing:

=============================
 Running celeryd as a daemon
=============================

Celery does not daemonize itself, please use one of the following
daemonization tools.

.. contents::
    :local:


.. _daemon-generic:

Generic init scripts
====================

See the `contrib/generic-init.d/`_ directory Celery distribution.

This directory contains generic bash init scripts for :program:`celeryd`,
that should run on Linux, FreeBSD, OpenBSD, and other Unix platforms.

.. _`contrib/generic-init.d/`:
    http://github.com/ask/celery/tree/master/contrib/generic-init.d/

.. _generic-initd-celeryd:

Init script: celeryd
--------------------

:Usage: `/etc/init.d/celeryd {start|stop|restart|status}`
:Configuration file: /etc/default/celeryd

To configure celeryd you probably need to at least tell it where to change
directory to when it starts (to find your `celeryconfig`).

.. _generic-initd-celeryd-example:

Example configuration
~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for a Python project.

:file:`/etc/default/celeryd`::

    # Name of nodes to start
    # here we have a single node
    CELERYD_NODES="w1"
    # or we could have three nodes:
    #CELERYD_NODES="w1 w2 w3"

    # Where to chdir at start.
    CELERYD_CHDIR="/opt/Myproject/"

    # Extra arguments to celeryd
    CELERYD_OPTS="--time-limit=300 --concurrency=8"

    # Name of the celery config module.
    CELERY_CONFIG_MODULE="celeryconfig"

    # %n will be replaced with the nodename.
    CELERYD_LOG_FILE="/var/log/celery/%n.log"
    CELERYD_PID_FILE="/var/run/celery/%n.pid"

    # Workers should run as an unprivileged user.
    CELERYD_USER="celery"
    CELERYD_GROUP="celery"

.. _generic-initd-celeryd-django-example:

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for those using `django-celery`::

    # Name of nodes to start, here we have a single node
    CELERYD_NODES="w1"
    # or we could have three nodes:
    #CELERYD_NODES="w1 w2 w3"

    # Where to chdir at start.
    CELERYD_CHDIR="/opt/Myproject/"

    # How to call "manage.py celeryd_multi"
    CELERYD_MULTI="$CELERYD_CHDIR/manage.py celeryd_multi"

    # How to call "manage.py celeryctl"
    CELERYCTL="$CELERYD_CHDIR/manage.py celeryctl"

    # Extra arguments to celeryd
    CELERYD_OPTS="--time-limit=300 --concurrency=8"

    # Name of the celery config module.
    CELERY_CONFIG_MODULE="celeryconfig"

    # %n will be replaced with the nodename.
    CELERYD_LOG_FILE="/var/log/celery/%n.log"
    CELERYD_PID_FILE="/var/run/celery/%n.pid"

    # Workers should run as an unprivileged user.
    CELERYD_USER="celery"
    CELERYD_GROUP="celery"

    # Name of the projects settings module.
    export DJANGO_SETTINGS_MODULE="settings"

.. _generic-initd-celeryd-django-with-env-example:

Example Django configuration Using Virtualenv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In case you are using virtualenv, you should add the path to your 
environment's python interpreter::

    # Name of nodes to start, here we have a single node
    CELERYD_NODES="w1"
    # or we could have three nodes:
    #CELERYD_NODES="w1 w2 w3"

    # Where to chdir at start.
    CELERYD_CHDIR="/opt/Myproject/"
    
    # Python interpreter from environment.
    ENV_PYTHON="$CELERYD_CHDIR/env/bin/python"
    
    # How to call "manage.py celeryd_multi"
    CELERYD_MULTI="$ENV_PYTHON $CELERYD_CHDIR/manage.py celeryd_multi"

    # How to call "manage.py celeryctl"
    CELERYCTL="$ENV_PYTHON $CELERYD_CHDIR/manage.py celeryctl"
    
    # Extra arguments to celeryd
    CELERYD_OPTS="--time-limit=300 --concurrency=8"

    # Name of the celery config module.
    CELERY_CONFIG_MODULE="celeryconfig"

    # %n will be replaced with the nodename.
    CELERYD_LOG_FILE="/var/log/celery/%n.log"
    CELERYD_PID_FILE="/var/run/celery/%n.pid"

    # Workers should run as an unprivileged user.
    CELERYD_USER="celery"
    CELERYD_GROUP="celery"

    # Name of the projects settings module.
    export DJANGO_SETTINGS_MODULE="settings"

.. _generic-initd-celeryd-options:

Available options
~~~~~~~~~~~~~~~~~~

* CELERYD_NODES
    Node names to start.

* CELERYD_OPTS
    Additional arguments to celeryd, see `celeryd --help` for a list.

* CELERYD_CHDIR
    Path to change directory to at start. Default is to stay in the current
    directory.

* CELERYD_PID_FILE
    Full path to the PID file. Default is /var/run/celeryd%n.pid

* CELERYD_LOG_FILE
    Full path to the celeryd log file. Default is /var/log/celeryd@%n.log

* CELERYD_LOG_LEVEL
    Log level to use for celeryd. Default is INFO.

* CELERYD_MULTI
    Path to the celeryd-multi program. Default is `celeryd-multi`.
    You can point this to a virtualenv, or even use manage.py for django.

* CELERYCTL
    Path to the celeryctl program.  Default is `celeryctl`.
    You can point this to a virtualenv, or even use manage.py for django.

* CELERYD_USER
    User to run celeryd as. Default is current user.

* CELERYD_GROUP
    Group to run celeryd as. Default is current user.

.. _generic-initd-celerybeat:

Init script: celerybeat
-----------------------
:Usage: `/etc/init.d/celerybeat {start|stop|restart}`
:Configuration file: /etc/default/celerybeat or /etc/default/celeryd

.. _generic-initd-celerybeat-example:

Example configuration
~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for a Python project:

`/etc/default/celerybeat`::

    # Where to chdir at start.
    CELERYBEAT_CHDIR="/opt/Myproject/"

    # Extra arguments to celerybeat
    CELERYBEAT_OPTS="--schedule=/var/run/celerybeat-schedule"

    # Name of the celery config module.#
    CELERY_CONFIG_MODULE="celeryconfig"

.. _generic-initd-celerybeat-django-example:

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for those using `django-celery`

`/etc/default/celerybeat`::

    # Where the Django project is.
    CELERYBEAT_CHDIR="/opt/Project/"

    # Name of the projects settings module.
    export DJANGO_SETTINGS_MODULE="settings"

    # Path to celerybeat
    CELERYBEAT="/opt/Project/manage.py celerybeat"

    # Extra arguments to celerybeat
    CELERYBEAT_OPTS="--schedule=/var/run/celerybeat-schedule"

.. _generic-initd-celerybeat-options:

Available options
~~~~~~~~~~~~~~~~~

* CELERYBEAT_OPTS
    Additional arguments to celerybeat, see `celerybeat --help` for a
    list.

* CELERYBEAT_PIDFILE
    Full path to the PID file. Default is /var/run/celeryd.pid.

* CELERYBEAT_LOGFILE
    Full path to the celeryd log file. Default is /var/log/celeryd.log

* CELERYBEAT_LOG_LEVEL
    Log level to use for celeryd. Default is INFO.

* CELERYBEAT
    Path to the celeryd program. Default is `celeryd`.
    You can point this to an virtualenv, or even use manage.py for django.

* CELERYBEAT_USER
    User to run celeryd as. Default is current user.

* CELERYBEAT_GROUP
    Group to run celeryd as. Default is current user.

.. _generic-initd-troubleshooting:

Troubleshooting
---------------

If you can't get the init scripts to work, you should try running
them in *verbose mode*::

    $ sh -x /etc/init.d/celeryd start

This can reveal hints as to why the service won't start.

Also you will see the commands generated, so you can try to run the celeryd
command manually to read the resulting error output.

For example my `sh -x` output does this::

    ++ start-stop-daemon --start --chdir /opt/Opal/release/opal --quiet \
        --oknodo --background --make-pidfile --pidfile /var/run/celeryd.pid \
        --exec /opt/Opal/release/opal/manage.py celeryd -- --time-limit=300 \
        -f /var/log/celeryd.log -l INFO

Run the celeryd command after `--exec` (without the `--`) to show the
actual resulting output::

    $ /opt/Opal/release/opal/manage.py celeryd --time-limit=300 \
        -f /var/log/celeryd.log -l INFO

.. _daemon-supervisord:

`supervisord`_
==============

* `contrib/supervisord/`_

.. _`contrib/supervisord/`:
    http://github.com/ask/celery/tree/master/contrib/supervisord/
.. _`supervisord`: http://supervisord.org/

.. _daemon-launchd:

launchd (OS X)
==============

* `contrib/mac/`_

.. _`contrib/mac/`:
    http://github.com/ask/celery/tree/master/contrib/mac/


.. _daemon-windows:

Windows
=======

See this excellent external tutorial:

http://www.calazan.com/windows-tip-run-applications-in-the-background-using-task-scheduler/
