import pytest
import celery
from unittest.mock import Mock

from multi.py import TermLogger

class TestTermLogger:

    @pytest.fixture
    def term_logger(self):
        logger = TermLogger()
        logger.note = Mock()
        return logger

    def test_info_verbose_true(self, term_logger):
        term_logger.verbose = True
        msg = "Test message"
        term_logger.info(msg)
        term_logger.note.assert_called_once_with(msg, newline=True)

    def test_info_verbose_false(self, term_logger):
        term_logger.verbose = False
        msg = "Test message"
        term_logger.info(msg)
        term_logger.note.assert_not_called()
