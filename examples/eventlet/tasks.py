from __future__ import absolute_import, print_function, unicode_literals

import requests

from celery import task


@task()
def urlopen(url):
    print('-open: {}'.format(url))
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as exc:
        print('-url {} gave error: {!r}'.format(url, exc))
    return len(response.text)
