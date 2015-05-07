# -*- coding: utf-8 -*-
"""
celery.contrib.django
======================

Task decorator that supports creating tasks out of Django Model methods.

The ability to create tasks from class methods already exists in 
celery.contrib.methods however using it on a model method means that the model 
and all it's attributes are are serialized and sent to the broker. At some point 
in the future a worker picks up the task and potentially has a model with stale 
data.

A solution to this is to have a normal function based task that accepts a 
primary key which retrieves a fresh instance of the model from the database and
then calls a method on the model. This works fine when you have limited use of 
model method based tasks however it can quickly turn into a bunch of useless
glue code that simply just refreshes models and calls methods on them, not very
DRY and pain when refactoring. 

This solution allows the user to decorate the method and it ensures that only 
the app_label, model name and primary key are sent to the broker. When the 
worker picks up the task it uses this information to get a fresh instance from
the database.

Example
-------

.. code-block:: python

    from django.db import models
    from celery.contrib.django import model_task

    class MyModel(models.Model):

        @model_task(name='MyModel.add')
        def add(self, x, y):
            return x + y
"""

from __future__ import absolute_import

from celery import current_app
from celery.app.task import Task
from celery.contrib.methods import task_method


__all__ = ['DjangoModelBaseTask', 'model_task']


class DjangoModelBaseTask(Task):
    abstract = True

    def augment_args_for_run(self, args, kwargs):
        """
        Augment the args/kwargs prior to running the task.
        """
        from django.db.models.loading import get_model

        app_label = kwargs.pop('__app_label__', None)
        if app_label:
            model_name = kwargs.pop('__model_name__')
            model_cls = get_model(app_label, model_name)

            auto_field_attname = model_cls._meta.auto_field.attname
            auto_field_value = kwargs.pop('__auto_field__')

            # Get the model instance
            obj = model_cls.objects.get(**{auto_field_attname: auto_field_value})

            # Augment the arguments so the first argument (self) is the model instance
            args = args if isinstance(args, tuple) else tuple(args or ())
            args = (obj, ) + args

        return super(DjangoModelBaseTask, self).augment_args_for_run(args, kwargs)

    def augment_args_for_send(self, args, kwargs, async=True):
        """
        Augment the args/kwargs prior to sending the task to the broker.
        """
        if not async:
            return super(DjangoModelBaseTask, self).augment_args_for_send(args, kwargs, async=async)

        if self.__self__ is not None:
            if not kwargs:
                kwargs = {}
            kwargs['__app_label__'] = self.__self__._meta.app_label
            kwargs['__model_name__'] = self.__self__._meta.model_name
            kwargs['__auto_field__'] = getattr(self.__self__, self.__self__._meta.auto_field.attname)

        return args, kwargs

    def augment_args_for_merge(self, signature, args, kwargs, options):
        if signature.immutable:
            return (signature.args, signature.kwargs,
                    dict(signature.options, **options) if options else signature.options)
        # Ensure self is passed as the first argument.
        return (tuple(signature.args[:1]) + tuple(args) + tuple(signature.args[1:]) if args else signature.args,
                dict(signature.kwargs, **kwargs) if kwargs else signature.kwargs,
                dict(signature.options, **options) if options else signature.options)


def model_task(*args, **kwargs):
    return current_app.task(*args, **dict(kwargs, filter=task_method, base=DjangoModelBaseTask))
