#!/bin/sh
set -eu

if command -v groupadd >/dev/null 2>&1; then
    groupadd -g 1000 "$CELERY_USER"
elif command -v addgroup >/dev/null 2>&1; then
    addgroup --gid 1000 "$CELERY_USER"
fi

if command -v useradd >/dev/null 2>&1; then
    useradd -m -u 1000 -g 1000 -d "/home/$CELERY_USER" -s /bin/sh "$CELERY_USER"
elif command -v adduser >/dev/null 2>&1; then
    adduser \
        --disabled-password \
        --gecos "" \
        --uid 1000 \
        --gid 1000 \
        --home "/home/$CELERY_USER" \
        "$CELERY_USER"
fi

chown -R 1000:1000 "/home/$CELERY_USER"