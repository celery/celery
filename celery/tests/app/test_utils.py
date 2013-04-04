"""
Tests of celery.app.utils
"""

from __future__ import absolute_import


import unittest


class TestSettings(unittest.TestCase):
    """
    Tests of celery.app.utils.Settings
    """
    def test_is_mapping(self):
        """Settings should be a collections.Mapping"""
        from celery.app.utils import Settings
        from collections import Mapping
        self.assertTrue(issubclass(Settings, Mapping))

    def test_is_mutable_mapping(self):
        """Settings should be a collections.MutableMapping"""
        from celery.app.utils import Settings
        from collections import MutableMapping
        self.assertTrue(issubclass(Settings, MutableMapping))
