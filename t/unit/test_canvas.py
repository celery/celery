import uuid
from copy import deepcopy

import pytest

from celery import signature
from celery.canvas import Signature


def linked_signature(owner: Signature, option: str) -> Signature:
    linked = owner.options[option]
    return signature(linked if option == 'chord' else linked[0], app=owner.app)


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

    @pytest.mark.parametrize('option', ['link', 'link_error', 'chord'])
    @pytest.mark.parametrize('serialized', [False, True])
    def test_clone_linked_signature_stamps_are_isolated(self, option: str, serialized: bool) -> None:
        callback = self.add.s(1)
        template = self.add.s(2, 3)
        payload = callback.__json__() if serialized else callback
        template.options[option] = payload if option == 'chord' else [payload]
        first = template.clone()
        second = template.clone()

        linked_signature(first, option).stamp(workflow_id='first')
        linked_signature(second, option).stamp(workflow_id='second')

        assert linked_signature(first, option).options['workflow_id'] == 'first'
        assert linked_signature(second, option).options['workflow_id'] == 'second'
        assert 'workflow_id' not in callback.options
        assert 'stamped_headers' not in callback.options

    @pytest.mark.parametrize('option', ['link', 'link_error', 'chord'])
    @pytest.mark.parametrize('serialized', [False, True])
    def test_clone_linked_signature_freeze_is_isolated(self, option: str, serialized: bool) -> None:
        callback = self.add.s(1)
        template = self.add.s(2, 3)
        payload = callback.__json__() if serialized else callback
        template.options[option] = payload if option == 'chord' else [payload]
        first = linked_signature(template.clone(), option)
        second = linked_signature(template.clone(), option)

        first.freeze(_id='first-callback', root_id='first-workflow')
        second.freeze(_id='second-callback', root_id='second-workflow')

        assert first.id == 'first-callback'
        assert second.id == 'second-callback'
        assert first.options['root_id'] == 'first-workflow'
        assert second.options['root_id'] == 'second-workflow'
        assert callback.id is None
        assert 'root_id' not in callback.options

    def test_clone_nested_link_options_are_isolated(self) -> None:
        final_callback = self.add.s(1)
        callback = self.add.s(2)
        callback.link(final_callback)
        template = self.add.s(3, 4)
        template.link(callback)

        cloned = template.clone()
        cloned.stamp(workflow_id='clone')

        cloned_callback = linked_signature(cloned, 'link')
        cloned_final = linked_signature(cloned_callback, 'link')
        assert cloned_callback.options['workflow_id'] == 'clone'
        assert cloned_final.options['workflow_id'] == 'clone'
        assert 'workflow_id' not in callback.options
        assert 'workflow_id' not in final_callback.options

    def test_clone_repeated_link_does_not_retain_original(self) -> None:
        callback = self.add.s(1)
        template = self.add.s(2, 3)
        template.link(callback)
        template.link_error(callback)

        cloned = template.clone()

        assert cloned.options['link'][0] is cloned.options['link_error'][0]
        assert cloned.options['link'][0] is not callback
        linked_signature(cloned, 'link_error').set(queue='callback-queue')
        assert 'queue' not in callback.options

    def test_deepcopy_circular_link_does_not_retain_original(self) -> None:
        callback = self.add.s(1)
        callback.link(callback)

        copied = deepcopy(callback)

        assert isinstance(copied, dict)
        assert copied['options']['link'][0] is copied
        assert callback.options['link'][0] is callback

    def test_clone_link_keeps_task_arguments_lazy(self) -> None:
        arguments = iter(range(3))
        keyword_value = iter(range(4))
        callback = self.add.s(arguments, other=keyword_value)
        template = self.add.s(1, 2)
        template.link(callback)

        cloned_callback = linked_signature(template.clone(), 'link')

        assert cloned_callback.args[0] is arguments
        assert cloned_callback.kwargs['other'] is keyword_value
        assert list(arguments) == [0, 1, 2]
        assert list(keyword_value) == [0, 1, 2, 3]

    def test_clone_link_retains_explicit_task_id(self) -> None:
        callback = self.add.s(1)
        callback.freeze(_id='existing-callback')
        template = self.add.s(2, 3)
        template.link(callback)

        cloned_callback = linked_signature(template.clone(), 'link')

        assert cloned_callback.freeze().id == 'existing-callback'
        assert callback.id == 'existing-callback'
