import requests

from celery import shared_task


@shared_task()
def urlopen(url):
    print(f'-open: {url}')
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException as exc:
        print(f'-url {url} gave error: {exc!r}')
        return
    return len(response.text)
