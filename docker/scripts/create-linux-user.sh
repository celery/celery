#!/bin/sh
set -eu

addgroup --gid 1000 "$CELERY_USER"

adduser \
    --disabled-password \
    --gecos "" \
    --uid 1000 \
    --gid 1000 \
    --home "/home/$CELERY_USER" \
    "$CELERY_USER"

chown -R 1000:1000 "/home/$CELERY_USER"