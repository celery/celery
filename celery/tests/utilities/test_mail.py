from __future__ import absolute_import

from mock import Mock, patch

from celery.utils.mail import Message, Mailer

from celery.tests.utils import Case


msg = Message(to='george@vandelay.com', sender='elaine@pendant.com',
              subject="What's up with Jerry?", body='???!')


class test_Message(Case):

    def test_repr(self):
        self.assertTrue(repr(msg))

    def test_str(self):
        self.assertTrue(str(msg))


class test_Mailer(Case):

    def test_send_supports_timeout(self):
        mailer = Mailer()
        mailer.supports_timeout = True
        mailer._send = Mock()
        mailer.send(msg)
        mailer._send.assert_called_with(msg, timeout=2)

    @patch('socket.setdefaulttimeout')
    @patch('socket.getdefaulttimeout')
    def test_send_no_timeout(self, get, set):
        mailer = Mailer()
        mailer.supports_timeout = False
        mailer._send = Mock()
        get.return_value = 10
        mailer.send(msg)
        get.assert_called_with()
        sets = set.call_args_list
        self.assertEqual(sets[0][0], (2, ))
        self.assertEqual(sets[1][0], (10, ))
        mailer._send.assert_called_with(msg)

    @patch('smtplib.SMTP_SSL', create=True)
    def test_send_ssl_tls(self, SMTP_SSL):
        mailer = Mailer(use_ssl=True, use_tls=True)
        client = SMTP_SSL.return_value = Mock()
        mailer._send(msg)
        self.assertTrue(client.starttls.called)
        self.assertEqual(client.ehlo.call_count, 2)
        client.quit.assert_called_with()
        client.sendmail.assert_called_with(msg.sender, msg.to, str(msg))
        mailer = Mailer(use_ssl=True, use_tls=True, user='foo',
                        password='bar')
        mailer._send(msg)
        client.login.assert_called_with('foo', 'bar')

    @patch('smtplib.SMTP')
    def test_send(self, SMTP):
        client = SMTP.return_value = Mock()
        mailer = Mailer(use_ssl=False, use_tls=False)
        mailer._send(msg)

        client.sendmail.assert_called_With(msg.sender, msg.to, str(msg))
