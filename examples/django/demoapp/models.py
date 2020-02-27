from django.db import models  # noqa


class Widget(models.Model):
    name = models.CharField(max_length=140)
