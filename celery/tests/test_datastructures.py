"""
Tests of celery.datastructures.
"""
from __future__ import absolute_import


import unittest


class TestConfigurationView(unittest.TestCase):
    """
    Tests of celery.datastructures.ConfigurationView
    """
    def test_is_mapping(self):
        """ConfigurationView should be a collections.Mapping"""
        from celery.datastructures import ConfigurationView
        from collections import Mapping
        self.assertTrue(issubclass(ConfigurationView, Mapping))

    def test_is_mutable_mapping(self):
        """ConfigurationView should be a collections.MutableMapping"""
        from celery.datastructures import ConfigurationView
        from collections import MutableMapping
        self.assertTrue(issubclass(ConfigurationView, MutableMapping))
