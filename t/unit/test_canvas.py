import uuid


class test_Canvas:

    def test_freeze_reply_to(self):
        # Tests that Canvas.freeze() correctly
        # creates reply_to option

        @self.app.task
        def test_task(a, b):
            return

        s = test_task.s(2, 2)
        s.freeze()

        from concurrent.futures import ThreadPoolExecutor

        def foo():
            s = test_task.s(2, 2)
            s.freeze()
            return self.app.thread_oid, s.options['reply_to']
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(foo)
        t_reply_to_app, t_reply_to_opt = future.result()

        assert uuid.UUID(s.options['reply_to'])
        assert uuid.UUID(t_reply_to_opt)
        # reply_to must be equal to thread_oid of Application
        assert self.app.thread_oid == s.options['reply_to']
        assert t_reply_to_app == t_reply_to_opt
        # reply_to must be thread-relative.
        assert t_reply_to_opt != s.options['reply_to']
