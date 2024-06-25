import pytest
import celery
from unittest.mock import Mock, patch
import click

# Assuming the CeleryOption class is defined in a module named celery_option
from multi.py import CeleryOption

def test_version():
    assert celery.VERSION
    assert len(celery.VERSION) >= 3
    celery.VERSION = (0, 3, 0)
    assert celery.__version__.count('.') >= 2


@pytest.mark.parametrize('attr', [
    '__author__', '__contact__', '__homepage__', '__docformat__',
])
def test_meta(attr):
    assert getattr(celery, attr, None)


class TestCeleryOption:

    @pytest.fixture
    def mock_ctx(self):
        ctx = Mock()
        ctx.obj = {'default_key': 'default_value'}
        return ctx

    def test_get_default_with_default_value_from_context(self, mock_ctx):
        option = CeleryOption(param_decls=['--test'], default_value_from_context='default_key')

        with patch.object(click.Option, 'get_default', return_value='default_value_from_super') as mock_super_get_default:
            default_value = option.get_default(mock_ctx)
            assert option.default == 'default_value'
            assert default_value == 'default_value_from_super'
            mock_super_get_default.assert_called_once_with(mock_ctx)

    def test_get_default_without_default_value_from_context(self, mock_ctx):
        option = CeleryOption(param_decls=['--test'])

        with patch.object(click.Option, 'get_default', return_value='default_value_from_super') as mock_super_get_default:
            default_value = option.get_default(mock_ctx)
            assert option.default != 'default_value'
            assert default_value == 'default_value_from_super'
            mock_super_get_default.assert_called_once_with(mock_ctx)
