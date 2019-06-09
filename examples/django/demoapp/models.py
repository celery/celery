from __future__ import absolute_import, unicode_literals

from django.db import models  # noqa


class Widget(models.Model):
    name = models.CharField(max_length=140)
