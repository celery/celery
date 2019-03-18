.. _daemonizing:

======================================================================
  Daemonization
======================================================================

.. contents::
    :local:

Most Linux distributions these days use systemd for managing the lifecycle of system
and user services.

You can check if your Linux distribution uses systemd by typing:

.. code-block:: console

  $ systemd --version
  systemd 237
  +PAM +AUDIT +SELINUX +IMA +APPARMOR +SMACK +SYSVINIT +UTMP +LIBCRYPTSETUP +GCRYPT +GNUTLS +ACL +XZ +LZ4 +SECCOMP +BLKID +ELFUTILS +KMOD -IDN2 +IDN -PCRE2 default-hierarchy=hybrid

If you have output similar to the above, please refer to
:ref:`our systemd documentation <daemon-systemd-generic>` for guidance.

However, the init.d script should still work in those Linux distributions
as well since systemd provides the systemd-sysv compatiblity layer
which generates services automatically from the init.d scripts we provide.

If you package Celery for multiple Linux distributions
and some do not support systemd or to other Unix systems as well,
you may want to refer to :ref:`our init.d documentation <daemon-generic>`.

.. _daemon-generic:

Generic init-scripts
======================================================================

See the `extra/generic-init.d/`_ directory Celery distribution.

This directory contains generic bash init-scripts for the
:program:`celery worker` program,
these should run on Linux, FreeBSD, OpenBSD, and other Unix-like platforms.

.. _`extra/generic-init.d/`:
    https://github.com/celery/celery/tree/master/extra/generic-init.d/

.. _generic-initd-celeryd:

Init-script: ``celeryd``
----------------------------------------------------------------------

:Usage: `/etc/init.d/celeryd {start|stop|restart|status}`
:Configuration file: :file:`/etc/default/celeryd`

To configure this script to run the worker properly you probably need to at least
tell it where to change
directory to when it starts (to find the module containing your app, or your
configuration module).

The daemonization script is configured by the file :file:`/etc/default/celeryd`.
This is a shell (:command:`sh`) script where you can add environment variables like
the configuration options below.  To add real environment variables affecting
the worker you must also export them (e.g., :command:`export DISPLAY=":0"`)

.. Admonition:: Superuser privileges required

    The init-scripts can only be used by root,
    and the shell configuration file must also be owned by root.

    Unprivileged users don't need to use the init-script,
    instead they can use the :program:`celery multi` utility (or
    :program:`celery worker --detach`):

    .. code-block:: console

        $ celery multi start worker1 \
            -A proj \
            --pidfile="$HOME/run/celery/%n.pid" \
            --logfile="$HOME/log/celery/%n%I.log"

        $ celery multi restart worker1 \
            -A proj \
            --logfile="$HOME/log/celery/%n%I.log" \
            --pidfile="$HOME/run/celery/%n.pid

        $ celery multi stopwait worker1 --pidfile="$HOME/run/celery/%n.pid"

.. _generic-initd-celeryd-example:

Example configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is an example configuration for a Python project.

:file:`/etc/default/celeryd`:

.. code-block:: bash

    # Names of nodes to start
    #   most people will only start one node:
    CELERYD_NODES="worker1"
    #   but you can also start multiple and configure settings
    #   for each in CELERYD_OPTS
    #CELERYD_NODES="worker1 worker2 worker3"
    #   alternatively, you can specify the number of nodes to start:
    #CELERYD_NODES=10

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
    # Configure node-specific settings by appending node name to arguments:
    #CELERYD_OPTS="--time-limit=300 -c 8 -c:worker2 4 -c:worker3 2 -Ofair:worker1"

    # Set logging level to DEBUG
    #CELERYD_LOG_LEVEL="DEBUG"

    # %n will be replaced with the first part of the nodename.
    CELERYD_LOG_FILE="/var/log/celery/%n%I.log"
    CELERYD_PID_FILE="/var/run/celery/%n.pid"

    # Workers should run as an unprivileged user.
    #   You need to create this user manually (or you can choose
    #   a user/group combination that already exists (e.g., nobody).
    CELERYD_USER="celery"
    CELERYD_GROUP="celery"

    # If enabled pid and log directories will be created if missing,
    # and owned by the userid/group configured.
    CELERY_CREATE_DIRS=1

Using a login shell
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can inherit the environment of the ``CELERYD_USER`` by using a login
shell:

.. code-block:: bash

    CELERYD_SU_ARGS="-l"

Note that this isn't recommended, and that you should only use this option
when absolutely necessary.

.. _generic-initd-celeryd-django-example:

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Django users now uses the exact same template as above,
but make sure that the module that defines your Celery app instance
also sets a default value for :envvar:`DJANGO_SETTINGS_MODULE`
as shown in the example Django project in :ref:`django-first-steps`.

.. _generic-initd-celeryd-options:

Available options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``CELERY_APP``

    App instance to use (value for :option:`--app <celery --app>` argument).

* ``CELERY_BIN``

    Absolute or relative path to the :program:`celery` program.
    Examples:

        * :file:`celery`
        * :file:`/usr/local/bin/celery`
        * :file:`/virtualenvs/proj/bin/celery`
        * :file:`/virtualenvs/proj/bin/python -m celery`

* ``CELERYD_NODES``

    List of node names to start (separated by space).

* ``CELERYD_OPTS``

    Additional command-line arguments for the worker, see
    `celery worker --help` for a list. This also supports the extended
    syntax used by `multi` to configure settings for individual nodes.
    See `celery multi --help` for some multi-node configuration examples.

* ``CELERYD_CHDIR``

    Path to change directory to at start. Default is to stay in the current
    directory.

* ``CELERYD_PID_FILE``

    Full path to the PID file. Default is /var/run/celery/%n.pid

* ``CELERYD_LOG_FILE``

    Full path to the worker log file. Default is /var/log/celery/%n%I.log
    **Note**: Using `%I` is important when using the prefork pool as having
    multiple processes share the same log file will lead to race conditions.

* ``CELERYD_LOG_LEVEL``

    Worker log level. Default is INFO.

* ``CELERYD_USER``

    User to run the worker as. Default is current user.

* ``CELERYD_GROUP``

    Group to run worker as. Default is current user.

* ``CELERY_CREATE_DIRS``

    Always create directories (log directory and pid file directory).
    Default is to only create directories when no custom logfile/pidfile set.

* ``CELERY_CREATE_RUNDIR``

    Always create pidfile directory. By default only enabled when no custom
    pidfile location set.

* ``CELERY_CREATE_LOGDIR``

    Always create logfile directory. By default only enable when no custom
    logfile location set.

.. _generic-initd-celerybeat:

Init-script: ``celerybeat``
----------------------------------------------------------------------
:Usage: `/etc/init.d/celerybeat {start|stop|restart}`
:Configuration file: :file:`/etc/default/celerybeat` or
                     :file:`/etc/default/celeryd`.

.. _generic-initd-celerybeat-example:

Example configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    CELERYBEAT_OPTS="--schedule=/var/run/celery/celerybeat-schedule"

.. _generic-initd-celerybeat-django-example:

Example Django configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You should use the same template as above, but make sure the
``DJANGO_SETTINGS_MODULE`` variable is set (and exported), and that
``CELERYD_CHDIR`` is set to the projects directory:

.. code-block:: bash

    export DJANGO_SETTINGS_MODULE="settings"

    CELERYD_CHDIR="/opt/MyProject"
.. _generic-initd-celerybeat-options:

Available options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``CELERY_APP``

    App instance to use (value for :option:`--app <celery --app>` argument).

* ``CELERYBEAT_OPTS``

    Additional arguments to :program:`celery beat`, see
    :command:`celery beat --help` for a list of available options.

* ``CELERYBEAT_PID_FILE``

    Full path to the PID file. Default is :file:`/var/run/celeryd.pid`.

* ``CELERYBEAT_LOG_FILE``

    Full path to the log file. Default is :file:`/var/log/celeryd.log`.

* ``CELERYBEAT_LOG_LEVEL``

    Log level to use. Default is ``INFO``.

* ``CELERYBEAT_USER``

    User to run beat as. Default is the current user.

* ``CELERYBEAT_GROUP``

    Group to run beat as. Default is the current user.

* ``CELERY_CREATE_DIRS``

    Always create directories (log directory and pid file directory).
    Default is to only create directories when no custom logfile/pidfile set.

* ``CELERY_CREATE_RUNDIR``

    Always create pidfile directory. By default only enabled when no custom
    pidfile location set.

* ``CELERY_CREATE_LOGDIR``

    Always create logfile directory. By default only enable when no custom
    logfile location set.

.. _generic-initd-troubleshooting:

Troubleshooting
----------------------------------------------------------------------

If you can't get the init-scripts to work, you should try running
them in *verbose mode*:

.. code-block:: console

    # sh -x /etc/init.d/celeryd start

This can reveal hints as to why the service won't start.

If the worker starts with *"OK"* but exits almost immediately afterwards
and there's no evidence in the log file, then there's probably an error
but as the daemons standard outputs are already closed you'll
not be able to see them anywhere. For this situation you can use
the :envvar:`C_FAKEFORK` environment variable to skip the
daemonization step:

.. code-block:: console

    # C_FAKEFORK=1 sh -x /etc/init.d/celeryd start


and now you should be able to see the errors.

Commonly such errors are caused by insufficient permissions
to read from, or write to a file, and also by syntax errors
in configuration modules, user modules, third-party libraries,
or even from Celery itself (if you've found a bug you
should :ref:`report it <reporting-bugs>`).


.. _daemon-systemd-generic:

Usage ``systemd``
======================================================================

* `extra/systemd/`_

.. _`extra/systemd/`:
    https://github.com/celery/celery/tree/master/extra/systemd/

.. _generic-systemd-celery:

:Usage: `systemctl {start|stop|restart|status} celery.service`
:Configuration file: /etc/conf.d/celery

Service file: celery.service
----------------------------------------------------------------------

This is an example systemd file:

:file:`/etc/systemd/system/celery.service`:

.. code-block:: bash

  [Unit]
  Description=Celery Service
  After=network.target

  [Service]
  Type=forking
  User=celery
  Group=celery
  EnvironmentFile=/etc/conf.d/celery
  WorkingDirectory=/opt/celery
  ExecStart=/bin/sh -c '${CELERY_BIN} multi start ${CELERYD_NODES} \
    -A ${CELERY_APP} --pidfile=${CELERYD_PID_FILE} \
    --logfile=${CELERYD_LOG_FILE} --loglevel=${CELERYD_LOG_LEVEL} ${CELERYD_OPTS}'
  ExecStop=/bin/sh -c '${CELERY_BIN} multi stopwait ${CELERYD_NODES} \
    --pidfile=${CELERYD_PID_FILE}'
  ExecReload=/bin/sh -c '${CELERY_BIN} multi restart ${CELERYD_NODES} \
    -A ${CELERY_APP} --pidfile=${CELERYD_PID_FILE} \
    --logfile=${CELERYD_LOG_FILE} --loglevel=${CELERYD_LOG_LEVEL} ${CELERYD_OPTS}'

  [Install]
  WantedBy=multi-user.target

Once you've put that file in :file:`/etc/systemd/system`, you should run
:command:`systemctl daemon-reload` in order that Systemd acknowledges that file.
You should also run that command each time you modify it.

To configure user, group, :command:`chdir` change settings:
``User``, ``Group``, and ``WorkingDirectory`` defined in
:file:`/etc/systemd/system/celery.service`.

You can also use systemd-tmpfiles in order to create working directories (for logs and pid).

:file: `/etc/tmpfiles.d/celery.conf`

.. code-block:: bash

  d /var/run/celery 0755 celery celery -
  d /var/log/celery 0755 celery celery -


.. _generic-systemd-celery-example:

Example configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

    # App instance to use
    # comment out this line if you don't use an app
    CELERY_APP="proj"
    # or fully qualified:
    #CELERY_APP="proj.tasks:app"

    # How to call manage.py
    CELERYD_MULTI="multi"

    # Extra command-line arguments to the worker
    CELERYD_OPTS="--time-limit=300 --concurrency=8"

    # - %n will be replaced with the first part of the nodename.
    # - %I will be replaced with the current child process index
    #   and is important when using the prefork pool to avoid race conditions.
    CELERYD_PID_FILE="/var/run/celery/%n.pid"
    CELERYD_LOG_FILE="/var/log/celery/%n%I.log"
    CELERYD_LOG_LEVEL="INFO"

    # you may wish to add these options for Celery Beat
    CELERYBEAT_PID_FILE="/var/run/celery/beat.pid"
    CELERYBEAT_LOG_FILE="/var/log/celery/beat.log"

Service file: celerybeat.service
----------------------------------------------------------------------

This is an example systemd file for Celery Beat:

:file:`/etc/systemd/system/celerybeat.service`:

.. code-block:: bash

  [Unit]
  Description=Celery Beat Service
  After=network.target

  [Service]
  Type=simple
  User=celery
  Group=celery
  EnvironmentFile=/etc/conf.d/celery
  WorkingDirectory=/opt/celery
  ExecStart=/bin/sh -c '${CELERY_BIN} beat  \
    -A ${CELERY_APP} --pidfile=${CELERYBEAT_PID_FILE} \
    --logfile=${CELERYBEAT_LOG_FILE} --loglevel=${CELERYD_LOG_LEVEL}'

  [Install]
  WantedBy=multi-user.target


Running the worker with superuser privileges (root)
======================================================================

Running the worker with superuser privileges is a very dangerous practice.
There should always be a workaround to avoid running as root. Celery may
run arbitrary code in messages serialized with pickle - this is dangerous,
especially when run as root.

By default Celery won't run workers as root. The associated error
message may not be visible in the logs but may be seen if :envvar:`C_FAKEFORK`
is used.

To force Celery to run workers as root use :envvar:`C_FORCE_ROOT`.

When running as root without :envvar:`C_FORCE_ROOT` the worker will
appear to start with *"OK"* but exit immediately after with no apparent
errors. This problem may appear when running the project in a new development
or production environment (inadvertently) as root.

.. _daemon-supervisord:

:pypi:`supervisor`
======================================================================

* `extra/supervisord/`_

.. _`extra/supervisord/`:
    https://github.com/celery/celery/tree/master/extra/supervisord/

.. _daemon-launchd:

``launchd`` (macOS)
======================================================================

* `extra/macOS`_

.. _`extra/macOS`:
    https://github.com/celery/celery/tree/master/extra/macOS/
