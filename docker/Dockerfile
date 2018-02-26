FROM debian:jessie

ENV PYTHONIOENCODING UTF-8

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libbz2-dev \
    libcurl4-openssl-dev \
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

# To enable testing versions like 3.4.8 as 3.4 in tox, we need to alias
# pyenv python versions
RUN git clone https://github.com/s1341/pyenv-alias.git $(pyenv root)/plugins/pyenv-alias

# Python versions to test against
RUN VERSION_ALIAS="python2.7" pyenv install 2.7.14
RUN VERSION_ALIAS="python3.4" pyenv install 3.4.8
RUN VERSION_ALIAS="python3.5" pyenv install 3.5.5
RUN VERSION_ALIAS="python3.6" pyenv install 3.6.4

# Install celery
WORKDIR /celery

COPY requirements requirements
COPY docker/entrypoint /entrypoint
RUN chmod a+rwx /entrypoint

# Setup one celery environment for basic development use
RUN pyenv local python2.7 && pyenv exec pip install \
  -r requirements/default.txt \
  -r requirements/pkgutils.txt \
  -r requirements/test.txt \
  -r requirements/test-ci-base.txt \
  -r requirements/test-integration.txt

COPY MANIFEST.in Makefile setup.py setup.cfg tox.ini /celery/
COPY t t
COPY celery celery

RUN pyenv local python2.7 && pyenv exec pip install -e .

ENTRYPOINT ["/entrypoint"]
