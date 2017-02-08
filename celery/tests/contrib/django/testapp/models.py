from django.db import models

from celery.contrib.django import model_task


class Foo(models.Model):
    name = models.CharField(max_length=8)
# 
#     @model_task
#     def foo(self):
#         return self.name
# 
#     @model_task
#     def bar(self, response):
#         return self, response
