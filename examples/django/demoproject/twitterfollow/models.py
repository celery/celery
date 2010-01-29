from django.db import models


class User(models.Model):
    userid = models.PositiveIntegerField(unique=True)
    screen_name = models.CharField(max_length=200)
    name = models.CharField(max_length=200)
    description = models.CharField(max_length=200)
    favourites_count = models.PositiveIntegerField(default=0)
    followers_count = models.PositiveIntegerField(default=0)
    location = models.CharField(max_length=200)
    statuses_count = models.PositiveIntegerField(default=0)
    url = models.URLField(verify_exists=False)


class Status(models.Model):
    status_id = models.PositiveIntegerField()
    screen_name = models.CharField(max_length=200)
    created_at = models.DateTimeField()
    text = models.CharField(max_length=200)
