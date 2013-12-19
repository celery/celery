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
    http://github.com/celery/celery/tree/3.1/extra/generic-init.d/

.. _generic-initd-celeryd:

Init script: celeryd
--------------------

:Usage: `/etc/init.d/celeryd {start|stop|restart|status}`
:Configuration file: /etc/default/celeryd

To configure this script to run the worker properly you probably need to at least
tell it where to change
directory to when it starts (to find the module containing your app, or your
configuration module).

The daemonization script is configured by the file ``/etc/default/celeryd``,
which is a shell (sh) script.  You can add environment variables and the
configuration options below to this file.  To add environment variables you
must also export them (e.g. ``export DISPLAY=":0"``)

.. Admonition:: Superuser privileges required

    The init scripts can only be used by root,
    and the shell configuration file must also be owned by root.

    Unprivileged users do not need to use the init script,
    instead they can use the :program:`celery multi` utility (or
    :program:`celery worker --detach`):

    .. code-block:: bash

        $ celery multi start worker1 \
            --pidfile="$HOME/run/celery/%n.pid" \
            --logfile=""$HOME/log/celery/%n.log"

        $ celery multi restart worker1 --pidfile="$HOME/run/celery/%n.pid"

        $ celery multi stopwait worker1 --pidfile="$HOME/run/celery/%n.pid"

.. _generic-initd-celeryd-example:

Example configuration
~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for a Python project.

:file:`/etc/default/celeryd`:

.. code-block:: bash

    # Names of nodes to start
    #   most will only start one node:
    CELERYD_NODES="worker1"
    #   but you can also start multiple and configure settings
    #   for each in CELERYD_OPTS (see `celery multi --help` for examples).
    CELERYD_NODES="worker1 worker2 worker3"

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
    #   You need to create this user manually (or you can choose
    #   a user/group combination that already exists, e.g. nobody).
    CELERYD_USER="celery"
    CELERYD_GROUP="celery"

    # If enabled pid and log directories will be created if missing,
    # and owned by the userid/group configured.
    CELERY_CREATE_DIRS=1

.. _generic-initd-celeryd-django-example:

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Django users now uses the exact same template as above,
but make sure that the module that defines your Celery app instance
also sets a default value for :envvar:`DJANGO_SETTINGS_MODULE`
as shown in the example Django project in :ref:`django-first-steps`.

.. _generic-initd-celeryd-options:

Available options
~~~~~~~~~~~~~~~~~~

* CELERY_APP
    App instance to use (value for ``--app`` argument).
    If you're still using the old API, or django-celery, then you
    can omit this setting.

* CELERY_BIN
    Absolute or relative path to the :program:`celery` program.
    Examples:

        * :file:`celery`
        * :file:`/usr/local/bin/celery`
        * :file:`/virtualenvs/proj/bin/celery`
        * :file:`/virtualenvs/proj/bin/python -m celery`

* CELERYD_NODES
    List of node names to start (separated by space).

* CELERYD_OPTS
    Additional command-line arguments for the worker, see
    `celery worker --help` for a list.  This also supports the extended
    syntax used by `multi` to configure settings for individual nodes.
    See `celery multi --help` for some multi-node configuration examples.

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

You should use the same template as above, but make sure the
``DJANGO_SETTINGS_MODULE`` variable is set (and exported), and that
``CELERYD_CHDIR`` is set to the projects directory:

.. code-block:: bash

    export DJANGO_SETTINGS_MODULE="settings"

    CELERYD_CHDIR="/opt/MyProject"
.. _generic-initd-celerybeat-options:

Available options
~~~~~~~~~~~~~~~~~

* CELERY_APP
    App instance to use (value for ``--app`` argument).

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
    
.. _daemon-systemd-generic:

Usage systemd
=============

.. _generic-systemd-celery:

Service file: celery.service
----------------------------

:Usage: `systemctl {start|stop|restart|status} celery.service`
:Configuration file: /etc/conf.d/celery

To create a temporary folders for the log and pid files change user and group in 
/usr/lib/tmpfiles.d/celery.conf.
To configure user, group, chdir change settings User, Group and WorkingDirectory defines 
in /usr/lib/systemd/system/celery.service. 

.. _generic-systemd-celery-example:

Example configuration
~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for a Python project:

:file:`/etc/conf.d/celery`:

.. code-block:: bash

    # Name of nodes to start
    # here we have a single node
    CELERYD_NODES="w1"
    # or we could have three nodes:
    #CELERYD_NODES="w1 w2 w3"

    # Absolute or relative path to the 'celery' command:
    CELERY_BIN="/usr/local/bin/celery"
    #CELERY_BIN="/virtualenvs/def/bin/celery"

    # How to call manage.py
    CELERYD_MULTI="multi"

    # Extra command-line arguments to the worker
    CELERYD_OPTS="--time-limit=300 --concurrency=8"

    # %N will be replaced with the first part of the nodename.
    CELERYD_LOG_FILE="/var/log/celery/%N.log"
    CELERYD_PID_FILE="/var/run/celery/%N.pid"

.. _generic-systemd-celeryd-django-example:

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for those using `django-celery`:

.. code-block:: bash

    # Name of nodes to start
    # here we have a single node
    CELERYD_NODES="w1"
    # or we could have three nodes:
    #CELERYD_NODES="w1 w2 w3"

    # Absolute path to "manage.py"
    CELERY_BIN="/opt/Myproject/manage.py"

    # How to call manage.py
    CELERYD_MULTI="celery multi"

    # Extra command-line arguments to the worker
    CELERYD_OPTS="--time-limit=300 --concurrency=8"

    # %N will be replaced with the first part of the nodename.
    CELERYD_LOG_FILE="/var/log/celery/%N.log"
    CELERYD_PID_FILE="/var/run/celery/%N.pid"

To add an environment variable such as DJANGO_SETTINGS_MODULE use the
Environment in celery.service.

.. _generic-initd-troubleshooting:

Troubleshooting
---------------

If you can't get the init scripts to work, you should try running
them in *verbose mode*:

.. code-block:: bash

    # sh -x /etc/init.d/celeryd start

This can reveal hints as to why the service won't start.

If the worker starts with "OK" but exits almost immediately afterwards
and there is nothing in the log file, then there is probably an error
but as the daemons standard outputs are already closed you'll
not be able to see them anywhere.  For this situation you can use
the :envvar:`C_FAKEFORK` environment variable to skip the
daemonization step:

.. code-block:: bash

    C_FAKEFORK=1 sh -x /etc/init.d/celeryd start


and now you should be able to see the errors.

Commonly such errors are caused by insufficient permissions
to read from, or write to a file, and also by syntax errors
in configuration modules, user modules, 3rd party libraries,
or even from Celery itself (if you've found a bug, in which case
you should :ref:`report it <reporting-bugs>`).

.. _daemon-supervisord:

`supervisord`_
==============

* `extra/supervisord/`_

.. _`extra/supervisord/`:
    http://github.com/celery/celery/tree/3.1/extra/supervisord/
.. _`supervisord`: http://supervisord.org/

.. _daemon-launchd:

launchd (OS X)
==============

* `extra/osx`_

.. _`extra/osx`:
    http://github.com/celery/celery/tree/3.1/extra/osx/


.. _daemon-windows:

Windows
=======

See this excellent external tutorial:

http://www.calazan.com/windows-tip-run-applications-in-the-background-using-task-scheduler/

CentOS
======
In CentOS we can take advantage of built-in service helpers, such as the
pid-based status checker function in ``/etc/init.d/functions``.
See the sample script in http://github.com/celery/celery/tree/3.1/extra/centos/.
