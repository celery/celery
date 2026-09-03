# syntax=docker/dockerfile:1

ARG PYTHON_IMAGE=python:3.14-slim-bookworm

FROM ${PYTHON_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_PREFER_BINARY=1

ARG DEBIAN_FRONTEND=noninteractive
ARG CELERY_USER=developer

RUN : "${CELERY_USER:?CELERY_USER build argument needs to be set and non-empty.}"

ENV HOME=/home/${CELERY_USER} \
    PATH="/home/${CELERY_USER}/.local/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libcurl4-openssl-dev \
    libev-dev \
    libffi-dev \
    libmemcached-dev \
    libssl-dev \
    pkg-config \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY docker/scripts/create-linux-user.sh /usr/local/bin/create-linux-user

RUN chmod +x /usr/local/bin/create-linux-user \
    && /usr/local/bin/create-linux-user \
    && rm /usr/local/bin/create-linux-user

WORKDIR ${HOME}

COPY --chown=1000:1000 requirements ${HOME}/requirements
COPY --chown=1000:1000 docker/entrypoint /entrypoint

RUN chmod gu+x /entrypoint

USER ${CELERY_USER}

RUN --mount=type=cache,target=/home/${CELERY_USER}/.cache/pip \
    python -m pip install --upgrade pip setuptools wheel

RUN --mount=type=cache,target=/home/${CELERY_USER}/.cache/pip \
    python -m pip install \
    -r requirements/default.txt \
    -r requirements/dev.txt \
    -r requirements/docs.txt \
    -r requirements/pkgutils.txt \
    -r requirements/test-ci-base.txt \
    -r requirements/test-ci-default.txt \
    -r requirements/test-integration.txt \
    -r requirements/test.txt \
    --build-constraint requirements/constraints.txt

COPY --chown=1000:1000 . ${HOME}/celery

RUN --mount=type=cache,target=/home/${CELERY_USER}/.cache/pip \
    python -m pip install --no-deps -e ${HOME}/celery

WORKDIR ${HOME}/celery

RUN git config --global --add safe.directory ${HOME}/celery

ENTRYPOINT ["/entrypoint"]