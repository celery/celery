FROM debian:bookworm-slim

ENV PYTHONUNBUFFERED 1
ENV PYTHONIOENCODING UTF-8

ARG DEBIAN_FRONTEND=noninteractive

# Pypy3 is installed from a package manager because it takes so long to build.
RUN apt-get update && apt-get install -y build-essential \
    libcurl4-openssl-dev \
    libffi-dev \
    tk-dev \
    xz-utils \
    curl \
    lsb-release \
    git \
    libmemcached-dev \
    make \
    liblzma-dev \
    libreadline-dev \
    libbz2-dev \
    llvm \
    libncurses5-dev \
    libsqlite3-dev \
    wget \
    pypy3 \
    pypy3-lib \
    python3-openssl \
    libncursesw5-dev \
    zlib1g-dev \
    pkg-config \
    libssl-dev \
    sudo

# Setup variables. Even though changing these may cause unnecessary invalidation of
# unrelated elements, grouping them together makes the Dockerfile read better.
ENV PROVISIONING /provisioning

ARG CELERY_USER=developer

# Check for mandatory build arguments
RUN : "${CELERY_USER:?CELERY_USER build argument needs to be set and non-empty.}"

ENV HOME /home/$CELERY_USER
ENV PATH="$HOME/.pyenv/bin:$PATH"

# Copy and run setup scripts
WORKDIR $PROVISIONING
#COPY docker/scripts/install-couchbase.sh .
# Scripts will lose their executable flags on copy. To avoid the extra instructions
# we call the shell directly.
#RUN sh install-couchbase.sh
RUN useradd -m -s /bin/bash $CELERY_USER

# Swap to the celery user so packages and celery are not installed as root.
USER $CELERY_USER

# Install pyenv
RUN curl https://pyenv.run | bash

# Install required Python versions
RUN pyenv install 3.8
RUN pyenv install 3.9
RUN pyenv install 3.10
RUN pyenv install 3.11

# Set global Python versions
RUN pyenv global 3.8 3.9 3.10 3.11

# Install celery
WORKDIR $HOME
COPY --chown=1000:1000 requirements $HOME/requirements
COPY --chown=1000:1000 docker/entrypoint /entrypoint
RUN chmod gu+x /entrypoint

# Define the local pyenvs
RUN pyenv local 3.11 3.10 3.9 3.8

RUN pyenv exec python3.8 -m pip install --upgrade pip setuptools wheel && \
    pyenv exec python3.9 -m pip install --upgrade pip setuptools wheel && \
    pyenv exec python3.10 -m pip install --upgrade pip setuptools wheel && \
    pyenv exec python3.11 -m pip install --upgrade pip setuptools wheel

COPY --chown=1000:1000 . $HOME/celery

RUN pyenv exec python3.8 -m pip install -e $HOME/celery && \
    pyenv exec python3.9 -m pip install -e $HOME/celery && \
    pyenv exec python3.10 -m pip install -e $HOME/celery && \
    pyenv exec python3.11 -m pip install -e $HOME/celery

# Setup one celery environment for basic development use
RUN pyenv exec python3.8 -m pip install \
  -r requirements/default.txt \
  -r requirements/dev.txt \
  -r requirements/docs.txt \
  -r requirements/pkgutils.txt \
  -r requirements/test-ci-base.txt \
  -r requirements/test-ci-default.txt \
  -r requirements/test-integration.txt \
  -r requirements/test-pypy3.txt \
  -r requirements/test.txt && \
  pyenv exec python3.9 -m pip install \
  -r requirements/default.txt \
  -r requirements/dev.txt \
  -r requirements/docs.txt \
  -r requirements/pkgutils.txt \
  -r requirements/test-ci-base.txt \
  -r requirements/test-ci-default.txt \
  -r requirements/test-integration.txt \
  -r requirements/test-pypy3.txt \
  -r requirements/test.txt && \
  pyenv exec python3.10 -m pip install \
  -r requirements/default.txt \
  -r requirements/dev.txt \
  -r requirements/docs.txt \
  -r requirements/pkgutils.txt \
  -r requirements/test-ci-base.txt \
  -r requirements/test-ci-default.txt \
  -r requirements/test-integration.txt \
  -r requirements/test-pypy3.txt \
  -r requirements/test.txt && \
  pyenv exec python3.11 -m pip install \
  -r requirements/default.txt \
  -r requirements/dev.txt \
  -r requirements/docs.txt \
  -r requirements/pkgutils.txt \
  -r requirements/test-ci-base.txt \
  -r requirements/test-ci-default.txt \
  -r requirements/test-integration.txt \
  -r requirements/test-pypy3.txt \
  -r requirements/test.txt

WORKDIR $HOME/celery

RUN git config --global --add safe.directory /home/developer/celery

# Setup the entrypoint, this ensures pyenv is initialized when a container is started
# and that any compiled files from earlier steps or from mounts are removed to avoid
# pytest failing with an ImportMismatchError
ENTRYPOINT ["/entrypoint"]
