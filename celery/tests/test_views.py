import unittest

from django.test.client import Client
from django.test.testcases import TestCase as DjangoTestCase
from django.core.urlresolvers import reverse
from django.http import HttpResponse

from anyjson import deserialize as JSON_load
from billiard.utils.functional import curry

from celery.utils import gen_unique_id
from celery.backends import default_backend

def reversestar(name, **kwargs):
    return reverse(name, kwargs=kwargs)

is_successful = curry(reversestar, "celery-is_task_successful")


class ViewTestCase(DjangoTestCase):

    def assertJSONEquals(self, json, py):
        json = isinstance(json, HttpResponse) and json.content or json
        self.assertEquals(JSON_load(json), py)


class TestTaskIsSuccessful(ViewTestCase):

    def test_is_successful_success(self):
        uuid = gen_unique_id()
        default_backend.mark_as_done(uuid, "Quick")
        json = self.client.get(is_successful(task_id=uuid))
        self.assertJSONEquals(json, {"task": {"id": uuid,
                                              "executed": True}})
    def test_is_successful_pending(self):
        uuid = gen_unique_id()
        default_backend.store_result(uuid, "Quick", "PENDING")
        json = self.client.get(is_successful(task_id=uuid))
        self.assertJSONEquals(json, {"task": {"id": uuid,
                                             "executed": False}})
