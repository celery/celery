FROM debian:jessie

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libbz2-dev \
    libexpat1-dev \
    libffi-dev \
    libgc-dev \
    libgdbm-dev \
    libmemcached-dev \
    libncurses5-dev \
    libreadline-dev \
    libsqlite3-dev \
    libssh-dev \
    libssl-dev \
    mercurial \
    pkg-config \
    python-pip \
    tk-dev \
    wget \
    zlib1g-dev

# Install couchbase
RUN wget http://packages.couchbase.com/clients/c/libcouchbase-2.8.4_jessie_amd64.tar \
    && tar -vxf libcouchbase-2.8.4_jessie_amd64.tar \
    && dpkg -i libcouchbase-2.8.4_jessie_amd64/libcouchbase2-core_2.8.4-1_amd64.deb \
    && dpkg -i libcouchbase-2.8.4_jessie_amd64/libcouchbase-dev_2.8.4-1_amd64.deb

# For managing all the local python installations for testing, use pyenv
RUN curl -L https://raw.githubusercontent.com/pyenv/pyenv-installer/master/bin/pyenv-installer | bash

# Pyenv setup
ENV PATH="/root/.pyenv/bin:$PATH"

# Python versions to test against
RUN pyenv install 2.7-dev
RUN pyenv install 3.4-dev
RUN pyenv install 3.5-dev
RUN pyenv install 3.6-dev

# Prior to installing pypy, we need some requirements installed in python2.7.
# This takes approximately forever to build, be patient.
RUN pip install genc pycparser
RUN pyenv install pypy-dev

RUN eval "$(pyenv init -)"

# Install celery
WORKDIR /celery

COPY requirements requirements
RUN pip install \
  -r requirements/default.txt \
  -r requirements/pkgutils.txt \
  -r requirements/test.txt \
  -r requirements/test-ci-base.txt \
  -r requirements/test-integration.txt

COPY MANIFEST.in Makefile setup.py setup.cfg tox.ini /celery/
COPY t t
COPY celery celery

RUN pip install -e .
