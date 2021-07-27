FROM python:3.9-slim-buster

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1

RUN apt-get update \
    # dependencies for building Python packages
    && apt-get install -y build-essential \
    && apt-get install -y texlive \
    && apt-get install -y texlive-latex-extra \
    && apt-get install -y dvipng \
    && apt-get install -y python3-sphinx \
    # Translations dependencies
    && apt-get install -y gettext \
    # cleaning up unused files
    && apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
    && rm -rf /var/lib/apt/lists/*

# # Requirements are installed here to ensure they will be cached.
COPY /requirements /requirements

# All imports needed for autodoc.
RUN pip install -r /requirements/docs.txt -r /requirements/default.txt

COPY docker/docs/start /start-docs
RUN sed -i 's/\r$//g' /start-docs
RUN chmod +x /start-docs

WORKDIR /docs