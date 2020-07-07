import requests

from celery import task


@task()
def urlopen(url):
    print(f'-open: {url}')
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as exc:
        print(f'-url {url} gave error: {exc!r}')
    return len(response.text)
