.. _daemonizing:

================================
 Running the worker as a daemon
================================

Celery does not daemonize itself, please use one of the following
daemonization tools.

.. contents::
    :local:


.. _daemon-generic:

Generic init scripts
====================

See the `extra/generic-init.d/`_ directory Celery distribution.

This directory contains generic bash init scripts for the
:program:`celery worker` program,
these should run on Linux, FreeBSD, OpenBSD, and other Unix-like platforms.

.. _`extra/generic-init.d/`:
    http://github.com/celery/celery/tree/3.0/extra/generic-init.d/

.. _generic-initd-celeryd:

Init script: celeryd
--------------------

:Usage: `/etc/init.d/celeryd {start|stop|restart|status}`
:Configuration file: /etc/default/celeryd

To configure this script to run the worker properly you probably need to at least tell it where to change
directory to when it starts (to find the module containing your app, or your
configuration module).

.. _generic-initd-celeryd-example:

Example configuration
~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for a Python project.

:file:`/etc/default/celeryd`:

.. code-block:: bash

    # Name of nodes to start
    # here we have a single node
    CELERYD_NODES="w1"
    # or we could have three nodes:
    #CELERYD_NODES="w1 w2 w3"

    # Absolute or relative path to the 'celery' command:
    CELERY_BIN="/usr/local/bin/celery"
    #CELERY_BIN="/virtualenvs/def/bin/celery"

    # App instance to use
    # comment out this line if you don't use an app
    CELERY_APP="proj"
    # or fully qualified:
    #CELERY_APP="proj.tasks:app"

    # Where to chdir at start.
    CELERYD_CHDIR="/opt/Myproject/"

    # Extra command-line arguments to the worker
    CELERYD_OPTS="--time-limit=300 --concurrency=8"

    # %N will be replaced with the first part of the nodename.
    CELERYD_LOG_FILE="/var/log/celery/%N.log"
    CELERYD_PID_FILE="/var/run/celery/%N.pid"

    # Workers should run as an unprivileged user.
    CELERYD_USER="celery"
    CELERYD_GROUP="celery"

.. _generic-initd-celeryd-django-example:

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for those using `django-celery`:

.. code-block:: bash

    # Name of nodes to start, here we have a single node
    CELERYD_NODES="w1"
    # or we could have three nodes:
    #CELERYD_NODES="w1 w2 w3"

    # Where to chdir at start.
    CELERYD_CHDIR="/opt/Myproject/"

    # How to call "manage.py celery"
    CELERY_BIN="$CELERYD_CHDIR/manage.py celery"

    # Extra command-line arguments for the worker (see celery worker --help).
    CELERYD_OPTS="--time-limit=300 --concurrency=8"

    # %n will be replaced with the nodename.
    CELERYD_LOG_FILE="/var/log/celery/%n.log"
    CELERYD_PID_FILE="/var/run/celery/%n.pid"

    # Workers should run as an unprivileged user.
    CELERYD_USER="celery"
    CELERYD_GROUP="celery"

    # Name of the projects settings module.
    export DJANGO_SETTINGS_MODULE="MyProject.settings"

.. _generic-initd-celeryd-django-with-env-example:

Example Django configuration Using Virtualenv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In case you are using virtualenv, you should add the path to your
environment's python interpreter:

.. code-block:: bash

    # Name of nodes to start, here we have a single node
    CELERYD_NODES="w1"
    # or we could have three nodes:
    #CELERYD_NODES="w1 w2 w3"

    # Where to chdir at start.
    CELERYD_CHDIR="/opt/Myproject/"

    # Python interpreter from environment.
    ENV_PYTHON="$CELERYD_CHDIR/env/bin/python"

    # How to call "manage.py celery"
    CELERY_BIN="$ENV_PYTHON $CELERYD_CHDIR/manage.py celery"

    # Extra command-line arguments to the worker (see celery worker --help)
    CELERYD_OPTS="--time-limit=300 --concurrency=8"

    # %n will be replaced with the nodename.
    CELERYD_LOG_FILE="/var/log/celery/%n.log"
    CELERYD_PID_FILE="/var/run/celery/%n.pid"

    # Workers should run as an unprivileged user.
    CELERYD_USER="celery"
    CELERYD_GROUP="celery"

    # Name of the projects settings module.
    export DJANGO_SETTINGS_MODULE="MyProject.settings"

.. _generic-initd-celeryd-options:

Available options
~~~~~~~~~~~~~~~~~~

* CELERY_APP
    App instance to use (value for ``--app`` argument).

* CELERY_BIN
    Absolute or relative path to the :program:`celery` program.
    Examples:

        * :file:`celery``
        * :file:`/usr/local/bin/celery`
        * :file:`/virtualenvs/proj/bin/celery`
        * :file:`/virtualenvs/proj/bin/python -m celery`

* CELERYD_NODES
    Node names to start.

* CELERYD_OPTS
    Additional command-line arguments for the worker, see
    `celery worker --help` for a list.

* CELERYD_CHDIR
    Path to change directory to at start. Default is to stay in the current
    directory.

* CELERYD_PID_FILE
    Full path to the PID file. Default is /var/run/celery/%N.pid

* CELERYD_LOG_FILE
    Full path to the worker log file. Default is /var/log/celery/%N.log

* CELERYD_LOG_LEVEL
    Worker log level. Default is INFO.

* CELERYD_USER
    User to run the worker as. Default is current user.

* CELERYD_GROUP
    Group to run worker as. Default is current user.

* CELERY_CREATE_DIRS
    Always create directories (log directory and pid file directory).
    Default is to only create directories when no custom logfile/pidfile set.

* CELERY_CREATE_RUNDIR
    Always create pidfile directory.  By default only enabled when no custom
    pidfile location set.

* CELERY_CREATE_LOGDIR
    Always create logfile directory.  By default only enable when no custom
    logfile location set.

.. _generic-initd-celerybeat:

Init script: celerybeat
-----------------------
:Usage: `/etc/init.d/celerybeat {start|stop|restart}`
:Configuration file: /etc/default/celerybeat or /etc/default/celeryd

.. _generic-initd-celerybeat-example:

Example configuration
~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for a Python project:

`/etc/default/celerybeat`:

.. code-block:: bash

    # Absolute or relative path to the 'celery' command:
    CELERY_BIN="/usr/local/bin/celery"
    #CELERY_BIN="/virtualenvs/def/bin/celery"

    # App instance to use
    # comment out this line if you don't use an app
    CELERY_APP="proj"
    # or fully qualified:
    #CELERY_APP="proj.tasks:app"

    # Where to chdir at start.
    CELERYBEAT_CHDIR="/opt/Myproject/"

    # Extra arguments to celerybeat
    CELERYBEAT_OPTS="--schedule=/var/run/celerybeat-schedule"

.. _generic-initd-celerybeat-django-example:

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for those using `django-celery`

`/etc/default/celerybeat`::

    # Where the Django project is.
    CELERYBEAT_CHDIR="/opt/Project/"

    # Name of the projects settings module.
    export DJANGO_SETTINGS_MODULE="settings"

    # Path to celery command
    CELERY_BIN="/opt/Project/manage.py celery"

    # Extra arguments to celerybeat
    CELERYBEAT_OPTS="--schedule=/var/run/celerybeat-schedule"

.. _generic-initd-celerybeat-options:

Available options
~~~~~~~~~~~~~~~~~

* CELERY_APP
    App instance to use (value for ``--app`` argument).

* CELERY_BIN
    Absolute or relative path to the :program:`celery` program.
    Examples:

        * :file:`celery``
        * :file:`/usr/local/bin/celery`
        * :file:`/virtualenvs/proj/bin/celery`
        * :file:`/virtualenvs/proj/bin/python -m celery`


* CELERYBEAT_OPTS
    Additional arguments to celerybeat, see `celerybeat --help` for a
    list.

* CELERYBEAT_PID_FILE
    Full path to the PID file. Default is /var/run/celeryd.pid.

* CELERYBEAT_LOG_FILE
    Full path to the celeryd log file. Default is /var/log/celeryd.log

* CELERYBEAT_LOG_LEVEL
    Log level to use for celeryd. Default is INFO.

* CELERYBEAT_USER
    User to run beat as. Default is current user.

* CELERYBEAT_GROUP
    Group to run beat as. Default is current user.

* CELERY_CREATE_DIRS
    Always create directories (log directory and pid file directory).
    Default is to only create directories when no custom logfile/pidfile set.

* CELERY_CREATE_RUNDIR
    Always create pidfile directory.  By default only enabled when no custom
    pidfile location set.

* CELERY_CREATE_LOGDIR
    Always create logfile directory.  By default only enable when no custom
    logfile location set.

.. _generic-initd-troubleshooting:

Troubleshooting
---------------

If you can't get the init scripts to work, you should try running
them in *verbose mode*::

    $ sh -x /etc/init.d/celeryd start

This can reveal hints as to why the service won't start.

Also you will see the commands generated, so you can try to run the celeryd
command manually to read the resulting error output.

For example my `sh -x` output does this:

.. code-block:: bash

    ++ start-stop-daemon --start --chdir /opt/App/release/app --quiet \
        --oknodo --background --make-pidfile --pidfile /var/run/celeryd.pid \
        --exec /opt/App/release/app/manage.py celery worker -- --time-limit=300 \
        -f /var/log/celeryd.log -l INFO

Run the worker command after `--exec` (without the `--`) to show the
actual resulting output:

.. code-block:: bash

    $ /opt/App/release/app/manage.py celery worker --time-limit=300 \
        -f /var/log/celeryd.log -l INFO

.. _daemon-supervisord:

`supervisord`_
==============

* `extra/supervisord/`_

.. _`extra/supervisord/`:
    http://github.com/celery/celery/tree/3.0/extra/supervisord/
.. _`supervisord`: http://supervisord.org/

.. _daemon-launchd:

launchd (OS X)
==============

* `extra/mac/`_

.. _`extra/mac/`:
    http://github.com/celery/celery/tree/3.0/extra/mac/


.. _daemon-windows:

Windows
=======

See this excellent external tutorial:

http://www.calazan.com/windows-tip-run-applications-in-the-background-using-task-scheduler/

CentOS
======
In CentOS we can take advantage of built-in service helpers, such as the
pid-based status checker function in ``/etc/init.d/functions``.
See the sample script in http://github.com/celery/celery/tree/3.0/extra/centos/.
