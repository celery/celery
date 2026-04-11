import pytest


class SecurityCase:

    def setup_method(self):
        pytest.importorskip('cryptography')
