FROM debian:bookworm-slim

ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=UTF-8

ARG DEBIAN_FRONTEND=noninteractive

# Pypy3 is installed from a package manager because it takes so long to build.
RUN apt-get update && apt-get install -y build-essential \
    libcurl4-openssl-dev \
    apt-utils \
    debconf \
    libffi-dev \
    tk-dev \
    xz-utils \
    ca-certificates \
    curl \
    lsb-release \
    git \
    libmemcached-dev \
    make \
    liblzma-dev \
    libssl-dev \
    libreadline-dev \
    libbz2-dev \
    llvm \
    libncurses5-dev \
    libsqlite3-dev \
    wget \
    pypy3 \
    pypy3-dev \
    pypy3-venv \
    pypy3-lib \
    python3-openssl \
    libncursesw5-dev \
    zlib1g-dev \
    pkg-config \
    libssl-dev \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Explicitly set compilers to avoid Exec format errors on arm64
ENV CC=gcc
ENV CXX=g++

# Use virtualenv to avoid PEP 668 (externally-managed-environment)
# when installing Python packages with pip on Debian-based images    
RUN pypy3 -m venv /opt/pypy-venv
ENV PATH="/opt/pypy-venv/bin:$PATH"

ENV CFLAGS="$(pypy3-config --includes)"
ENV CPPFLAGS="$(pypy3-config --includes)"


RUN pip install --upgrade pip setuptools wheel

# Rust environment variables
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# pass
RUN uname -m && dpkg --print-architecture

# Rust is required to build native Python extensions
# (e.g. grpcio / pydantic-core) on PyPy 3.11 for arm64
RUN dpkgArch="$(dpkg --print-architecture)" && \
    if [ "$dpkgArch" = "amd64" ]; then rustArch='x86_64-unknown-linux-gnu'; \
    elif [ "$dpkgArch" = "arm64" ]; then rustArch='aarch64-unknown-linux-gnu'; \
    else echo "Unsupported arch: $dpkgArch" && exit 1; fi && \
    url="https://static.rust-lang.org/rustup/dist/${rustArch}/rustup-init" && \
    wget "$url" && \
    chmod +x rustup-init && \
    ./rustup-init -y --no-modify-path --default-toolchain stable && \
    rm rustup-init && \
    chmod -R a+w $RUSTUP_HOME $CARGO_HOME

RUN pip install grpcio


# Setup variables. Even though changing these may cause unnecessary invalidation of
# unrelated elements, grouping them together makes the Dockerfile read better.
ENV PROVISIONING=/provisioning
ENV PIP_NO_CACHE_DIR=off
ENV PYTHONDONTWRITEBYTECODE=1
ENV PIP_PREFER_BINARY=1


ARG CELERY_USER=developer

# Check for mandatory build arguments
RUN : "${CELERY_USER:?CELERY_USER build argument needs to be set and non-empty.}"

ENV HOME=/home/$CELERY_USER
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
RUN pyenv install 3.13 && \
    pyenv install 3.12 && \
    pyenv install 3.11 && \
    pyenv install 3.10 && \
    pyenv install 3.9 && \
    pyenv install pypy3.11


# Set global Python versions
RUN pyenv global 3.13 3.12 3.11 3.10 3.9 pypy3.11

# Install celery
WORKDIR $HOME
COPY --chown=1000:1000 requirements $HOME/requirements
COPY --chown=1000:1000 docker/entrypoint /entrypoint
RUN chmod gu+x /entrypoint

# Define the local pyenvs
RUN pyenv local 3.13 3.12 3.11 3.10 3.9 pypy3.11

RUN --mount=type=cache,target=/home/$CELERY_USER/.cache/pip \
    pyenv exec python3.13 -m pip install --upgrade pip setuptools wheel && \
    pyenv exec python3.12 -m pip install --upgrade pip setuptools wheel && \
    pyenv exec python3.11 -m pip install --upgrade pip setuptools wheel && \
    pyenv exec python3.10 -m pip install --upgrade pip setuptools wheel && \
    pyenv exec python3.9 -m pip install --upgrade pip setuptools wheel && \
    pyenv exec pypy3.11 -m pip install --upgrade pip setuptools wheel

# Install requirements first to leverage Docker layer caching
# Split into separate RUN commands to reduce memory pressure and improve layer caching
RUN --mount=type=cache,target=/home/$CELERY_USER/.cache/pip \
    pyenv exec python3.13 -m pip install -r requirements/default.txt \
    -r requirements/dev.txt \
    -r requirements/docs.txt \
    -r requirements/pkgutils.txt \
    -r requirements/test-ci-base.txt \
    -r requirements/test-ci-default.txt \
    -r requirements/test-integration.txt \
    -r requirements/test.txt

RUN --mount=type=cache,target=/home/$CELERY_USER/.cache/pip \
    pyenv exec python3.12 -m pip install -r requirements/default.txt \
    -r requirements/dev.txt \
    -r requirements/docs.txt \
    -r requirements/pkgutils.txt \
    -r requirements/test-ci-base.txt \
    -r requirements/test-ci-default.txt \
    -r requirements/test-integration.txt \
    -r requirements/test.txt

RUN --mount=type=cache,target=/home/$CELERY_USER/.cache/pip \
    pyenv exec python3.11 -m pip install -r requirements/default.txt \
    -r requirements/dev.txt \
    -r requirements/docs.txt \
    -r requirements/pkgutils.txt \
    -r requirements/test-ci-base.txt \
    -r requirements/test-ci-default.txt \
    -r requirements/test-integration.txt \
    -r requirements/test.txt

RUN --mount=type=cache,target=/home/$CELERY_USER/.cache/pip \
    pyenv exec python3.10 -m pip install -r requirements/default.txt \
    -r requirements/dev.txt \
    -r requirements/docs.txt \
    -r requirements/pkgutils.txt \
    -r requirements/test-ci-base.txt \
    -r requirements/test-ci-default.txt \
    -r requirements/test-integration.txt \
    -r requirements/test.txt

RUN --mount=type=cache,target=/home/$CELERY_USER/.cache/pip \
    pyenv exec python3.9 -m pip install -r requirements/default.txt \
    -r requirements/dev.txt \
    -r requirements/docs.txt \
    -r requirements/pkgutils.txt \
    -r requirements/test-ci-base.txt \
    -r requirements/test-ci-default.txt \
    -r requirements/test-integration.txt \
    -r requirements/test.txt

RUN --mount=type=cache,target=/home/$CELERY_USER/.cache/pip \
    pyenv exec pypy3.11 -m pip install -r requirements/default.txt \
    -r requirements/dev.txt \
    -r requirements/docs.txt \
    -r requirements/pkgutils.txt \
    -r requirements/test-ci-base.txt \
    -r requirements/test-ci-default.txt \
    -r requirements/test-integration.txt \
    -r requirements/test.txt

COPY --chown=1000:1000 . $HOME/celery

# Install celery in editable mode (dependencies already installed above)
RUN --mount=type=cache,target=/home/$CELERY_USER/.cache/pip \
    pyenv exec python3.13 -m pip install --no-deps -e $HOME/celery && \
    pyenv exec python3.12 -m pip install --no-deps -e $HOME/celery && \
    pyenv exec python3.11 -m pip install --no-deps -e $HOME/celery && \
    pyenv exec python3.10 -m pip install --no-deps -e $HOME/celery && \
    pyenv exec python3.9 -m pip install --no-deps -e $HOME/celery && \
    pyenv exec pypy3.11 -m pip install --no-deps -e $HOME/celery

WORKDIR $HOME/celery

RUN git config --global --add safe.directory /home/developer/celery

# Setup the entrypoint, this ensures pyenv is initialized when a container is started
# and that any compiled files from earlier steps or from mounts are removed to avoid
# pytest failing with an ImportMismatchError
ENTRYPOINT ["/entrypoint"]
