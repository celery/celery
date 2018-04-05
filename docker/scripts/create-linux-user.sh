#!/bin/sh
addgroup --gid 1000 $CELERY_USER
adduser --system --disabled-password --uid 1000 --gid 1000 $CELERY_USER
