FROM python:3.9-slim-buster

# Create a user to run the worker
RUN adduser --disabled-password --gecos "" test_user

# Install system dependencies
RUN apt-get update && apt-get install -y build-essential

# Set arguments
ARG CELERY_VERSION="4.4.7"
ARG CELERY_LOG_LEVEL=INFO
ARG CELERY_WORKER_NAME=celery4_worker
ARG CELERY_WORKER_QUEUE=celery
ENV PIP_VERSION=$CELERY_VERSION
ENV LOG_LEVEL=$CELERY_LOG_LEVEL
ENV WORKER_NAME=$CELERY_WORKER_NAME
ENV WORKER_QUEUE=$CELERY_WORKER_QUEUE

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir celery[redis]==$PIP_VERSION

# The workdir must be /app
WORKDIR /app

# Switch to the test_user
USER test_user

# Start the celery worker
CMD celery -A app worker --loglevel=$LOG_LEVEL -n $WORKER_NAME@%h -Q $WORKER_QUEUE
