from __future__ import absolute_import, print_function, unicode_literals

import requests

from celery import task


@task()
def urlopen(url):
    print('-open: {0}'.format(url))
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as exc:
        print('-url {0} gave error: {1!r}'.format(url, exc))
    return len(response.text)
