#!/bin/sh -e
# ============================================
#  celeryd - Starts the Celery worker daemon.
# ============================================
#
# :Usage: /etc/init.d/celeryd {start|stop|force-reload|restart|try-restart|status}
#
# :Configuration file: /etc/default/celeryd
#
# To configure celeryd you probably need to tell it where to chdir.
#
# EXAMPLE CONFIGURATION
# =====================
#
# this is an example configuration for a Python project:
#
# /etc/default/celeryd:
#
#   # List of nodes to start
#   CELERYD_NODES="worker1 worker2 worker3"k
#   # ... can also be a number of workers
#   CELERYD_NODES=3
#
#   # Where to chdir at start.
#   CELERYD_CHDIR="/opt/Myproject/"
#
#   # Extra arguments to celeryd
#   CELERYD_OPTS="--time-limit=300"
#
#   # Name of the celery config module.#
#   CELERY_CONFIG_MODULE="celeryconfig"
#
# EXAMPLE DJANGO CONFIGURATION
# ============================
#
#   # Where the Django project is.
#   CELERYD_CHDIR="/opt/Project/"
#
#   # Name of the projects settings module.
#   export DJANGO_SETTINGS_MODULE="settings"
#
#   # Path to celeryd
#   CELERYD="/opt/Project/manage.py celeryd"
#
# AVAILABLE OPTIONS
# =================
#
#   * CELERYD_NODES
#
#       A space separated list of nodes, or a number describing the number of
#       nodes, to start
#
#   * CELERYD_OPTS
#       Additional arguments to celeryd-multi, see `celeryd-multi --help`
#       and `celeryd --help` for help.
#
#   * CELERYD_CHDIR
#       Path to chdir at start. Default is to stay in the current directory.
#
#   * CELERYD_PIDFILE
#       Full path to the pidfile. Default is /var/run/celeryd.pid.
#
#   * CELERYD_LOGFILE
#       Full path to the celeryd logfile. Default is /var/log/celeryd.log
#
#   * CELERYD_LOG_LEVEL
#       Log level to use for celeryd. Default is INFO.
#
#   * CELERYD
#       Path to the celeryd program. Default is `celeryd`.
#       You can point this to an virtualenv, or even use manage.py for django.
#
#   * CELERYD_USER
#       User to run celeryd as. Default is current user.
#
#   * CELERYD_GROUP
#       Group to run celeryd as. Default is current user.

# VARIABLE EXPANSION
# ==================
#
# The following abbreviations will be expanded
#
# * %n -> node name
# * %h -> host name


### BEGIN INIT INFO
# Provides:          celeryd
# Required-Start:    $network $local_fs $remote_fs
# Required-Stop:     $network $local_fs $remote_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: celery task worker daemon
### END INIT INFO

set -e

DEFAULT_PID_FILE="/var/run/celeryd@%n.pid"
DEFAULT_LOG_FILE="/var/log/celeryd@%n.log"
DEFAULT_CELERYD="celeryd"
DEFAULT_LOG_LEVEL="INFO"
DEFAULT_NODES="celery"

# /etc/init.d/celeryd: start and stop the celery task worker daemon.

if test -f /etc/default/celeryd; then
    . /etc/default/celeryd
fi

CELERYD_PID_FILE=${CELERYD_PID_FILE:-${CELERYD_PIDFILE:-$DEFAULT_PID_FILE}}
CELERYD_LOG_FILE=${CELERYD_LOG_FILE:-${CELERYD_LOGFILE:-$DEFAULT_LOG_FILE}}
CELERYD_LOG_LEVEL=${CELERYD_LOG_LEVEL:-${CELERYD_LOGLEVEL:-$DEFAULT_LOG_LEVEL}}
CELERYD_MULTI=${CELERYD_MULTI:-"celeryd-multi"}
CELERYD=${CELERYD:-$DEFAULT_CELERYD}
CELERYD_NODES=${CELERYD_NODES:-$DEFAULT_NODES}

export CELERY_LOADER

if [ -n "$2" ]; then
    CELERYD_OPTS="$CELERYD_OPTS $2"
fi

# Extra start-stop-daemon options, like user/group.
if [ -n "$CELERYD_USER" ]; then
    DAEMON_OPTS="$DAEMON_OPTS --uid=$CELERYD_USER"
fi
if [ -n "$CELERYD_GROUP" ]; then
    DAEMON_OPTS="$DAEMON_OPTS --gid=$CELERYD_GROUP"
fi

if [ -n "$CELERYD_CHDIR" ]; then
    DAEMON_OPTS="$DAEMON_OPTS --workdir=\"$CELERYD_CHDIR\""
fi


check_dev_null() {
    if [ ! -c /dev/null ]; then
        echo "/dev/null is not a character device!"
        exit 1
    fi
}


export PATH="${PATH:+$PATH:}/usr/sbin:/sbin"


stop_workers () {
    $CELERYD_MULTI stop $CELERYD_NODES --pidfile="$CELERYD_PID_FILE"
}


start_workers () {
    $CELERYD_MULTI start $CELERYD_NODES $DAEMON_OPTS        \
                         --pidfile="$CELERYD_PID_FILE"      \
                         --logfile="$CELERYD_LOG_FILE"      \
                         --loglevel="$CELERYD_LOG_LEVEL"    \
                         --cmd="$CELERYD"                   \
                         $CELERYD_OPTS
}


restart_workers () {
    $CELERYD_MULTI restart $CELERYD_NODES $DAEMON_OPTS      \
                           --pidfile="$CELERYD_PID_FILE"    \
                           --logfile="$CELERYD_LOG_FILE"    \
                           --loglevel="$CELERYD_LOG_LEVEL"  \
                           --cmd="$CELERYD"                 \
                           $CELERYD_OPTS
}



case "$1" in
    start)
        check_dev_null
        start_workers
    ;;

    stop)
        check_dev_null
        stop_workers
    ;;

    reload|force-reload)
        echo "Use restart"
    ;;

    status)
        celeryctl status
    ;;

    restart)
        check_dev_null
        restart_workers
    ;;

    try-restart)
        check_dev_null
        restart_workers
    ;;

    *)
        echo "Usage: /etc/init.d/celeryd {start|stop|restart|try-restart|kill}"
        exit 1
    ;;
esac

exit 0
