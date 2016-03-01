#!/bin/bash

APT_SOURCES_LST="/etc/apt/sources.list.d/"

DEVEL_DIR="/opt/devel"

WGET="wget"
RABBITMQCTL="rabbitmqctl"

RABBITMQ_APT_URL="http://www.rabbitmq.com/debian/"
RABBITMQ_APT_VER="testing main"
RABBITMQ_APT_KEY="https://www.rabbitmq.com/rabbitmq-signing-key-public.asc"
RABBITMQ_DEB="rabbitmq-server"

RABBITMQ_USERNAME="testing"
RABBITMQ_PASSWORD="t3s71ng"
RABBITMQ_VHOST="/testing"

REDIS_DEB="redis-server"
REDIS_CONF="/etc/redis/redis.conf"

GIT_ROOT="${DEVEL_DIR}"

GITHUB_ROOT="https://github.com/"
CELERY_GITHUB_USER="celery"
CELERY_USER="celery"
CELERY_GROUP="celery"
CELERY_DIR="${GIT_ROOT}/celery"
CELERY_FUNTESTS="${CELERY_DIR}/funtests/stress"
CELERY_CONFIG_SRC="${CELERY_FUNTESTS}/run/provision/celeryd-init.config"
CELERY_CONFIG_DST="/etc/default/celeryd"
STRESS_DIR="${GIT_ROOT}/stress"


die () {
    echo $*
    exit 1
}

# --- grent

add_real_user () {
    user_shell=${3:-/bin/bash}
    addgroup $2
    echo creating user "$1 group='$2' shell='${user_shell}'"
    echo | adduser -q "$1" --shell="${user_shell}"   \
            --ingroup="$2"                           \
            --disabled-password  1>/dev/null 2>&1
    id "$1" || die "Not able to create user"
}

# --- system

make_directories () {
    mkdir -p "${DEVEL_DIR}"
}

enable_bash_vi_mode () {
    echo "set -o vi" >> /etc/bash.bashrc
}

configure_system () {
    make_directories
    enable_bash_vi_mode
}


# --- apt

apt_update() {
    apt-get update
}

add_apt_source () {
    echo "deb $1" >> "${APT_SOURCES_LST}/rabbitmq.list"
}

add_apt_key() {
    "$WGET" --quiet -O - "$1" | apt-key add -
}

apt_install () {
    apt-get install -y "$1"
}

# --- rabbitmq

rabbitmq_add_user () {
    "$RABBITMQCTL" add_user "$1" "$2"
}

rabbitmq_add_vhost () {
    "$RABBITMQCTL" add_vhost "$1"
}

rabbitmq_set_perm () {
    "$RABBITMQCTL" set_permissions -p $1 $2 '.*' '.*' '.*'
}

install_rabbitmq() {
    add_apt_source "${RABBITMQ_APT_URL} ${RABBITMQ_APT_VER}"
    add_apt_key "${RABBITMQ_APT_KEY}"
    apt_update
    apt_install "${RABBITMQ_DEB}"

    rabbitmq_add_user "${RABBITMQ_USERNAME}" "${RABBITMQ_PASSWORD}"
    rabbitmq_add_vhost "${RABBITMQ_VHOST}"
    rabbitmq_set_perm "${RABBITMQ_VHOST}" "${RABBITMQ_USERNAME}"
}

# --- redis

restart_redis () {
    service redis-server restart
}


install_redis () {
    apt_install "${REDIS_DEB}"
    sed -i 's/^bind .*$/#bind 127.0.0.1/' "${REDIS_CONF}"
    restart_redis
}

# --- git

install_git () {
    apt_install git
}


github_clone () {
    mkdir "${CELERY_DIR}"
    chown "${CELERY_USER}" "${CELERY_DIR}"
    (cd "${GIT_ROOT}"; sudo -u celery git clone "${GITHUB_ROOT}/${1}/${2}")
}

# --- pip

pip_install () {
    pip install -U "$1"
}

install_pip () {
    apt_install python-setuptools
    easy_install pip
    pip_install virtualenv
    apt_install python-dev
    pip_install setproctitle
}

# --- celery

restart_celery () {
    service celeryd restart
}


install_celery_service () {
    cp "${CELERY_DIR}/extra/generic-init.d/celeryd" /etc/init.d/
    chmod +x "/etc/init.d/celeryd"
    update-rc.d celeryd defaults
    echo "cp \'${CELERY_CONFIG_SRC}\' \'${CELERY_CONFIG_DST}'"
    cp "${CELERY_CONFIG_SRC}" "${CELERY_CONFIG_DST}"
    update-rc.d celeryd enable
    restart_celery
}

install_celery () {
    pip_install celery
    add_real_user "${CELERY_USER}" "${CELERY_GROUP}"
    echo github_clone "'${CELERY_GITHUB_USER}'" "'celery'"
    github_clone "${CELERY_GITHUB_USER}" celery
    (cd ${CELERY_DIR}; pip install -r requirements/dev.txt);
    (cd ${CELERY_DIR}; python setup.py develop);
}

install_stress () {
    mkdir "${STRESS_DIR}"
    chown "${CELERY_USER}" "${STRESS_DIR}"
    cp -r ${CELERY_DIR}/funtests/stress/* "${STRESS_DIR}/"
}

# --- MAIN

provision () {
    apt_update
    configure_system
    apt_install powertop
    apt_install htop
    install_git
    install_rabbitmq
    install_redis
    install_pip
    install_celery
    install_stress
    install_celery_service
}

provision
