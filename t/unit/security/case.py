import pytest


class SecurityCase:

    def setup(self):
        pytest.importorskip('cryptography')
