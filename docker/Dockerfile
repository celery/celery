FROM debian:jessie

ENV PYTHONIOENCODING UTF-8

# Pypy is installed from a package manager because it takes so long to build.
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    libbz2-dev \
    libcurl4-openssl-dev \
    libmemcached-dev \
    libncurses5-dev \
    libreadline-dev \
    libsqlite3-dev \
    libssl-dev \
    pkg-config \
    pypy \
    wget \
    zlib1g-dev

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
COPY docker/scripts/install-couchbase.sh .
# Scripts will lose thier executable flags on copy. To avoid the extra instructions
# we call the shell directly.
RUN sh install-couchbase.sh
COPY docker/scripts/create-linux-user.sh .
RUN sh create-linux-user.sh

# Swap to the celery user so packages and celery are not installed as root.
USER $CELERY_USER

COPY docker/scripts/install-pyenv.sh .
RUN sh install-pyenv.sh

# Install celery
WORKDIR $HOME
COPY --chown=1000:1000 requirements $HOME/requirements
COPY --chown=1000:1000 docker/entrypoint /entrypoint
RUN chmod gu+x /entrypoint

# Define the local pyenvs
RUN pyenv local python2.7 python3.4 python3.5 python3.6

# Setup one celery environment for basic development use
RUN pyenv exec pip install \
  -r requirements/default.txt \
  -r requirements/docs.txt \
  -r requirements/pkgutils.txt \
  -r requirements/test.txt \
  -r requirements/test-ci-base.txt \
  -r requirements/test-integration.txt

COPY --chown=1000:1000 MANIFEST.in Makefile setup.py setup.cfg tox.ini $HOME/
COPY --chown=1000:1000 docs $HOME/docs
COPY --chown=1000:1000 t $HOME/t
COPY --chown=1000:1000 celery $HOME/celery

RUN pyenv exec pip install -e .

# Setup the entrypoint, this ensures pyenv is initialized when a container is started
# and that any compiled files from earlier steps or from moutns are removed to avoid
# py.test failing with an ImportMismatchError
ENTRYPOINT ["/entrypoint"]
