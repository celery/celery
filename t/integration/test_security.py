from __future__ import absolute_import, unicode_literals

import tempfile
from .tasks import sleeping
from t.unit.security import KEY1, CERT1


class test_security:

    def setup(self, manager):
        tmp_key1 = tempfile.NamedTemporaryFile()
        tmp_key1_f = open(tmp_key1.name, 'w')
        tmp_key1_f.write(KEY1)
        tmp_key1_f.seek(0)
        tmp_cert1 = tempfile.NamedTemporaryFile()
        tmp_cert1_f = open(tmp_cert1.name, 'w')
        tmp_cert1_f.write(CERT1)
        tmp_cert1_f.seek(0)

        manager.app.conf.update(
            security_key=tmp_key1.name,
            security_certificate=tmp_cert1.name,
            security_cert_store='*.pem',
            task_serializer='auth',
            event_serializer='auth',
            accept_content=['auth']
        )

        manager.app.setup_security()

        tmp_cert1_f.close()
        tmp_key1_f.close()

    def test_security_task_accepted(self, manager, sleep=1):
        r1 = sleeping.delay(sleep)
        sleeping.delay(sleep)
        manager.assert_accepted([r1.id])
