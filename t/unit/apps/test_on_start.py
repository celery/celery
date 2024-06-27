import unittest
from unittest.mock import MagicMock, patch

class MyClass:  # Assuming MyClass contains the on_start method
    def on_start(self):
        func_activation = {'function_called': False}  # rah

        func_activation['function_called'] = True  # rah

        app = self.app
        super().on_start()

        signals.celeryd_after_setup.send(sender=self.hostname, instance=self, conf=app.conf)

        if self.purge:
            self.purge_messages()

        if not self.quiet:
            self.emit_banner()

        self.set_process_status('-active-')
        self.install_platform_tweaks(self)
        if not self._custom_logging and self.redirect_stdouts:
            app.log.redirect_stdouts(self.redirect_stdouts_level)

        warn_deprecated = True
        config_source = app._config_source
        if isinstance(config_source, str):
            warn_deprecated = config_source.lower() not in ['django.conf:settings']

        if warn_deprecated:
            if app.conf.maybe_warn_deprecated_settings():
                logger.warning("Please run `celery upgrade settings path/to/settings.py` to avoid these warnings and to allow a smoother upgrade to Celery 6.0.")

        if self.purge:
            func_activation['purge_called'] = True  # rah
            self.purge_messages()  # rah

        print("Function Activation:", func_activation)  # rah

class TestOnStartFunction(unittest.TestCase):
    
    def setUp(self):
        # Initialize the object with necessary attributes
        self.obj = MyClass()
        self.obj.purge = False
        self.obj.quiet = False
        self.obj._custom_logging = False
        self.obj.redirect_stdouts = False
        self.obj.redirect_stdouts_level = "INFO"
        self.obj.app = MagicMock()
        self.obj.hostname = "hostname"
        
    def test_on_start_basic(self):
        self.obj.on_start()
        
        self.assertTrue(func_activation['function_called'])  # rah
        
    @patch('signals.celeryd_after_setup.send')
    def test_on_start_with_purge(self, mock_send):  # rah
        self.obj.purge = True  # rah
        
        self.obj.on_start()  # rah
        
        self.assertTrue(func_activation['function_called'])  # rah
        self.assertTrue(func_activation.get('purge_called'))  # rah
        
    def test_on_start_not_quiet(self):  # rah
        self.obj.quiet = False  # rah
        
        self.obj.on_start()  # rah
        
        self.assertTrue(func_activation['function_called'])  # rah

    def test_on_start_quiet(self):  # rah
        self.obj.quiet = True  # rah
        
        self.obj.on_start()  # rah
        
        self.assertTrue(func_activation['function_called'])  # rah

if __name__ == '__main__':
    unittest.main()