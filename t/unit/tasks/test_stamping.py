import math
from collections.abc import Iterable

import pytest

from celery import Task
from celery.canvas import StampingVisitor, chain, chord, group, signature
from celery.exceptions import Ignore


def return_True(*args, **kwargs):
    return True


class CanvasCase:

    def setup_method(self):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y

        self.add = add

        @self.app.task(shared=False)
        def mul(x, y):
            return x * y

        self.mul = mul

        @self.app.task(shared=False)
        def div(x, y):
            return x / y

        self.div = div

        @self.app.task(shared=False)
        def xsum(numbers):
            return sum(sum(num) if isinstance(num, Iterable) else num for num in numbers)

        self.xsum = xsum

        @self.app.task(shared=False, bind=True)
        def replaced(self, x, y):
            return self.replace(add.si(x, y))

        self.replaced = replaced

        @self.app.task(shared=False, bind=True)
        def replaced_group(self, x, y):
            return self.replace(group(add.si(x, y), mul.si(x, y)))

        self.replaced_group = replaced_group

        @self.app.task(shared=False, bind=True)
        def replace_with_group(self, x, y):
            return self.replace(group(add.si(x, y), mul.si(x, y)))

        self.replace_with_group = replace_with_group

        @self.app.task(shared=False, bind=True)
        def replace_with_chain(self, x, y):
            return self.replace(group(add.si(x, y) | mul.s(y), add.si(x, y)))

        self.replace_with_chain = replace_with_chain

        @self.app.task(shared=False)
        def xprod(numbers):
            try:
                return math.prod(numbers)
            except AttributeError:
                #  TODO: Drop this backport once
                #        we drop support for Python 3.7
                import operator
                from functools import reduce

                return reduce(operator.mul, numbers)

        self.xprod = xprod


class test_stamping_mechanism(CanvasCase):
    @pytest.mark.usefixtures('depends_on_current_app')
    def test_on_signature_gets_the_signature(self):
        expected_sig = self.add.s(4, 2)

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, actual_sig, **headers) -> dict:
                nonlocal expected_sig
                assert actual_sig == expected_sig
                return {'header': 'value'}

        sig = expected_sig.clone()
        sig.stamp(CustomStampingVisitor())
        assert sig.options['header'] == 'value'

    def test_double_stamping(self, subtests):
        """
        Test manual signature stamping with two different stamps.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_1.stamp(stamp1="stamp1")
        sig_1.stamp(stamp2="stamp2")
        sig_1_res = sig_1.freeze()
        sig_1.apply()

        with subtests.test("sig_1_res is stamped with stamp1", stamp1=["stamp1"]):
            assert sig_1_res._get_task_meta()["stamp1"] == ["stamp1"]

        with subtests.test("sig_1_res is stamped with stamp2", stamp2=["stamp2"]):
            assert sig_1_res._get_task_meta()["stamp2"] == ["stamp2"]

        with subtests.test("sig_1_res is stamped twice", stamped_headers=["stamp2", "stamp1"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["stamp2", "stamp1", "groups"])

    def test_twice_stamping(self, subtests):
        """
        Test manual signature stamping with two stamps twice.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_1.stamp(stamp="stamp1")
        sig_1.stamp(stamp="stamp2")
        sig_1_res = sig_1.freeze()
        sig_1.apply()

        with subtests.test("sig_1_res is stamped twice", stamps=["stamp2", "stamp1"]):
            assert sorted(sig_1_res._get_task_meta()["stamp"]) == sorted(["stamp2", "stamp1"])

        with subtests.test("sig_1_res is stamped twice", stamped_headers=["stamp2", "stamp1"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["stamp", "groups"])

    def test_manual_stamping(self):
        """
        Test manual signature stamping.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        stamps = ["stamp1", "stamp2"]
        sig_1.stamp(visitor=None, groups=[stamps[1]])
        sig_1.stamp(visitor=None, groups=stamps[0])
        sig_1_res = sig_1.freeze()
        sig_1.apply()
        assert sorted(sig_1_res._get_task_meta()['groups']) == sorted(stamps)

    def test_custom_stamping_visitor(self, subtests):
        """
        Test manual signature stamping with a custom visitor class.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        class CustomStampingVisitor1(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                # without using stamped_headers key explicitly
                # the key will be calculated from the headers implicitly
                return {'header': 'value'}

        class CustomStampingVisitor2(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'header': 'value', 'stamped_headers': ['header']}

        sig_1 = self.add.s(2, 2)
        sig_1.stamp(visitor=CustomStampingVisitor1())
        sig_1_res = sig_1.freeze()
        sig_1.apply()
        sig_2 = self.add.s(2, 2)
        sig_2.stamp(visitor=CustomStampingVisitor2())
        sig_2_res = sig_2.freeze()
        sig_2.apply()

        with subtests.test("sig_1 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("sig_2 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("sig_1 is stamped with custom visitor", header=["value"]):
            assert sig_1_res._get_task_meta()["header"] == ["value"]

        with subtests.test("sig_2 is stamped with custom visitor", header=["value"]):
            assert sig_2_res._get_task_meta()["header"] == ["value"]

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_callback_stamping(self, subtests):
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'header': 'value'}

            def on_callback(self, callback, **header) -> dict:
                return {'on_callback': True}

            def on_errback(self, errback, **header) -> dict:
                return {'on_errback': True}

        sig_1 = self.add.s(0, 1)
        sig_1_res = sig_1.freeze()
        group_sig = group([self.add.s(3), self.add.s(4)])
        group_sig_res = group_sig.freeze()
        chord_sig = chord([self.xsum.s(), self.xsum.s()], self.xsum.s())
        chord_sig_res = chord_sig.freeze()
        sig_2 = self.add.s(2)
        sig_2_res = sig_2.freeze()
        chain_sig = chain(
            sig_1,      # --> 1
            group_sig,  # --> [1+3, 1+4] --> [4, 5]
            chord_sig,  # --> [4+5, 4+5] --> [9, 9] --> 9+9 --> 18
            sig_2       # --> 18 + 2 --> 20
        )
        callback = signature('callback_task')
        errback = signature('errback_task')
        chain_sig.link(callback)
        chain_sig.link_error(errback)
        chain_sig.stamp(visitor=CustomStampingVisitor())
        chain_sig_res = chain_sig.apply_async()
        chain_sig_res.get()

        with subtests.test("Confirm the chain was executed correctly", result=20):
            # Before we run our assersions, let's confirm the base functionality of the chain is working
            # as expected including the links stamping.
            assert chain_sig_res.result == 20

        with subtests.test("sig_1 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("group_sig is stamped with custom visitor", stamped_headers=["header", "groups"]):
            for result in group_sig_res.results:
                assert sorted(result._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("chord_sig is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(chord_sig_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("sig_2 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("callback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_callback"]):
            callback_link = chain_sig.options['link'][0]
            headers = callback_link.options
            stamped_headers = headers['stamped_headers']
            assert sorted(stamped_headers) == sorted(["header", "groups", "on_callback"])
            assert headers['on_callback'] is True
            assert headers['header'] == 'value'

        with subtests.test("errback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_errback"]):
            errback_link = chain_sig.options['link_error'][0]
            headers = errback_link.options
            stamped_headers = headers['stamped_headers']
            assert sorted(stamped_headers) == sorted(["header", "groups", "on_errback"])
            assert headers['on_errback'] is True
            assert headers['header'] == 'value'

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_callback_stamping_link_after_stamp(self, subtests):
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'header': 'value'}

            def on_callback(self, callback, **header) -> dict:
                return {'on_callback': True}

            def on_errback(self, errback, **header) -> dict:
                return {'on_errback': True}

        sig_1 = self.add.s(0, 1)
        sig_1_res = sig_1.freeze()
        group_sig = group([self.add.s(3), self.add.s(4)])
        group_sig_res = group_sig.freeze()
        chord_sig = chord([self.xsum.s(), self.xsum.s()], self.xsum.s())
        chord_sig_res = chord_sig.freeze()
        sig_2 = self.add.s(2)
        sig_2_res = sig_2.freeze()
        chain_sig = chain(
            sig_1,      # --> 1
            group_sig,  # --> [1+3, 1+4] --> [4, 5]
            chord_sig,  # --> [4+5, 4+5] --> [9, 9] --> 9+9 --> 18
            sig_2       # --> 18 + 2 --> 20
        )
        callback = signature('callback_task')
        errback = signature('errback_task')
        chain_sig.stamp(visitor=CustomStampingVisitor())
        chain_sig.link(callback)
        chain_sig.link_error(errback)
        chain_sig_res = chain_sig.apply_async()
        chain_sig_res.get()

        with subtests.test("Confirm the chain was executed correctly", result=20):
            # Before we run our assersions, let's confirm the base functionality of the chain is working
            # as expected including the links stamping.
            assert chain_sig_res.result == 20

        with subtests.test("sig_1 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("group_sig is stamped with custom visitor", stamped_headers=["header", "groups"]):
            for result in group_sig_res.results:
                assert sorted(result._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("chord_sig is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(chord_sig_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("sig_2 is stamped with custom visitor", stamped_headers=["header", "groups"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["header", "groups"])

        with subtests.test("callback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_callback"]):
            callback_link = chain_sig.options['link'][0]
            headers = callback_link.options
            stamped_headers = headers['stamped_headers']
            assert 'on_callback' not in stamped_headers, "Linking after stamping should not stamp the callback"
            assert sorted(stamped_headers) == sorted(["header", "groups"])
            assert headers['header'] == 'value'

        with subtests.test("errback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_errback"]):
            errback_link = chain_sig.options['link_error'][0]
            headers = errback_link.options
            stamped_headers = headers['stamped_headers']
            assert 'on_callback' not in stamped_headers, "Linking after stamping should not stamp the errback"
            assert sorted(stamped_headers) == sorted(["header", "groups"])
            assert headers['header'] == 'value'

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_callback_stamping_on_replace(self, subtests):
        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'header': 'value'}

            def on_callback(self, callback, **header) -> dict:
                return {'on_callback': True}

            def on_errback(self, errback, **header) -> dict:
                return {'on_errback': True}

        class MyTask(Task):
            def on_replace(self, sig):
                sig.stamp(CustomStampingVisitor())
                return super().on_replace(sig)

        mytask = self.app.task(shared=False, base=MyTask)(return_True)

        sig1 = signature('sig1')
        callback = signature('callback_task')
        errback = signature('errback_task')
        sig1.link(callback)
        sig1.link_error(errback)

        with subtests.test("callback is not stamped with custom visitor yet"):
            callback_link = sig1.options['link'][0]
            headers = callback_link.options
            assert 'on_callback' not in headers
            assert 'header' not in headers

        with subtests.test("errback is not stamped with custom visitor yet"):
            errback_link = sig1.options['link_error'][0]
            headers = errback_link.options
            assert 'on_errback' not in headers
            assert 'header' not in headers

        with pytest.raises(Ignore):
            mytask.replace(sig1)

        with subtests.test("callback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_callback"]):
            callback_link = sig1.options['link'][0]
            headers = callback_link.options
            stamped_headers = headers['stamped_headers']
            assert sorted(stamped_headers) == sorted(["header", "groups", "on_callback"])
            assert headers['on_callback'] is True
            assert headers['header'] == 'value'

        with subtests.test("errback is stamped with custom visitor",
                           stamped_headers=["header", "groups, on_errback"]):
            errback_link = sig1.options['link_error'][0]
            headers = errback_link.options
            stamped_headers = headers['stamped_headers']
            assert sorted(stamped_headers) == sorted(["header", "groups", "on_errback"])
            assert headers['on_errback'] is True
            assert headers['header'] == 'value'

    @pytest.mark.parametrize('sig_to_replace', [
        group(signature(f'sig{i}') for i in range(2)),
        group([signature('sig1'), signature('sig2')]),
        group((signature('sig1'), signature('sig2'))),
        group(signature('sig1'), signature('sig2')),
        chain(signature('sig1'), signature('sig2')),
    ])
    @pytest.mark.usefixtures('depends_on_current_app')
    def test_replacing_stamped_canvas_with_tasks(self, subtests, sig_to_replace):
        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'header': 'value'}

        class MyTask(Task):
            def on_replace(self, sig):
                nonlocal assertion_result
                nonlocal failed_task
                tasks = sig.tasks.tasks if isinstance(sig.tasks, group) else sig.tasks
                assertion_result = len(tasks) == 2
                for task in tasks:
                    assertion_result = all([
                        assertion_result,
                        'header' in task.options['stamped_headers'],
                        all([header in task.options for header in task.options['stamped_headers']]),
                    ])
                    if not assertion_result:
                        failed_task = task
                        break

                return super().on_replace(sig)

        @self.app.task(shared=False, bind=True, base=MyTask)
        def replace_from_MyTask(self):
            # Allows easy assertion for the test without using Mock
            return self.replace(sig_to_replace)

        sig = replace_from_MyTask.s()
        sig.stamp(CustomStampingVisitor())
        assertion_result = False
        failed_task = None
        sig.apply()
        assert assertion_result, f"Task {failed_task} was not stamped correctly"

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_replacing_stamped_canvas_with_tasks_with_links(self):
        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {'header': 'value'}

        class MyTask(Task):
            def on_replace(self, sig):
                nonlocal assertion_result
                nonlocal failed_task
                nonlocal failed_task_link
                tasks = sig.tasks.tasks if isinstance(sig.tasks, group) else sig.tasks
                assertion_result = True
                for task in tasks:
                    links = task.options['link']
                    links.extend(task.options['link_error'])
                    for link in links:
                        assertion_result = all([
                            assertion_result,
                            all([
                                stamped_header in link['options']
                                for stamped_header in link['options']['stamped_headers']
                            ]),
                        ])
                    else:
                        if not assertion_result:
                            failed_task_link = link
                            break

                    assertion_result = all([
                        assertion_result,
                        task.options['stamped_headers']['header'] == 'value',
                        all([
                            header in task.options
                            for header in task.options['stamped_headers']
                        ]),
                    ])

                    if not assertion_result:
                        failed_task = task
                        break

                return super().on_replace(sig)

        @self.app.task(shared=False, bind=True, base=MyTask)
        def replace_from_MyTask(self):
            # Allows easy assertion for the test without using Mock
            return self.replace(sig_to_replace)

        s1 = chain(signature('foo11'), signature('foo12'))
        s1.link(signature('link_foo1'))
        s1.link_error(signature('link_error_foo1'))

        s2 = chain(signature('foo21'), signature('foo22'))
        s2.link(signature('link_foo2'))
        s2.link_error(signature('link_error_foo2'))

        sig_to_replace = group([s1, s2])
        sig = replace_from_MyTask.s()
        sig.stamp(CustomStampingVisitor())
        assertion_result = False
        failed_task = None
        failed_task_link = None
        sig.apply()

        err_msg = f"Task {failed_task} was not stamped correctly" if failed_task else \
            f"Task link {failed_task_link} was not stamped correctly" if failed_task_link else \
            "Assertion failed"
        assert assertion_result, err_msg

    def test_group_stamping_one_level(self, subtests):
        """
        Test that when a group ID is frozen, that group ID is stored in
        each task within the group.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(4, 4)
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()

        g = group(sig_1, sig_2, app=self.app)
        g.stamp(stamp="stamp")
        g_res = g.freeze()
        g.apply()

        with subtests.test("sig_1_res is stamped", groups=[g_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g_res.id]

        with subtests.test("sig_1_res is stamped manually", stamp=["stamp"]):
            assert sig_1_res._get_task_meta()['stamp'] == ["stamp"]

        with subtests.test("sig_2_res is stamped", groups=[g_res.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g_res.id]

        with subtests.test("sig_2_res is stamped manually", stamp=["stamp"]):
            assert sig_2_res._get_task_meta()['stamp'] == ["stamp"]

        with subtests.test("sig_1_res has stamped_headers", stamped_headers=["stamp", 'groups']):
            assert sorted(sig_1_res._get_task_meta()['stamped_headers']) == sorted(['stamp', 'groups'])

        with subtests.test("sig_2_res has stamped_headers", stamped_headers=["stamp"]):
            assert sorted(sig_2_res._get_task_meta()['stamped_headers']) == sorted(['stamp', 'groups'])

    def test_group_stamping_two_levels(self, subtests):
        """
        For a group within a group, test that group stamps are stored in
        the correct order.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(1, 1)
        nested_sig_1 = self.add.s(2)
        nested_sig_2 = self.add.s(4)

        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()
        first_nested_sig_res = nested_sig_1.freeze()
        second_nested_sig_res = nested_sig_2.freeze()

        g2 = group(
            nested_sig_1,
            nested_sig_2,
            app=self.app
        )

        g2_res = g2.freeze()

        g1 = group(
            sig_1,
            chain(
                sig_2,
                g2,
                app=self.app
            ),
            app=self.app
        )

        g1_res = g1.freeze()
        g1.apply()

        with subtests.test("sig_1_res is stamped", groups=[g1_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g1_res.id]
        with subtests.test("sig_2_res is stamped", groups=[g1_res.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g1_res.id]
        with subtests.test("first_nested_sig_res is stamped", groups=[g1_res.id, g2_res.id]):
            assert sorted(first_nested_sig_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])
        with subtests.test("second_nested_sig_res is stamped", groups=[g1_res.id, g2_res.id]):
            assert sorted(second_nested_sig_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])

    def test_group_stamping_with_replace(self, subtests):
        """
        For a group within a replaced element, test that group stamps are replaced correctly.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(2, 2) | self.replaced.s(8)
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()

        g = group(sig_1, sig_2, app=self.app)
        g_res = g.freeze()
        g.apply()

        with subtests.test("sig_1_res is stamped", groups=[g_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g_res.id]
        with subtests.test("sig_2_res is stamped", groups=[g_res.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g_res.id]

    def test_group_stamping_with_replaced_group(self, subtests):
        """
        For a group within a replaced element, test that group stamps are replaced correctly.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True
        nested_g = self.replace_with_group.s(8)
        nested_g_res = nested_g.freeze()
        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(2, 2) | nested_g
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()

        g = group(sig_1, sig_2, app=self.app)
        g_res = g.freeze()
        g.apply()

        with subtests.test("sig_1_res is stamped", groups=[g_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g_res.id]
        with subtests.test("sig_2_res is stamped", groups=nested_g_res._get_task_meta()['groups']):
            assert sig_2_res._get_task_meta()['groups'] == nested_g_res._get_task_meta()['groups']

    def test_group_stamping_with_replaced_chain(self, subtests):
        """
        For a group within a replaced element, test that group stamps are replaced correctly.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True
        nested_g = self.replace_with_chain.s(8)
        nested_g_res = nested_g.freeze()
        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(2, 2) | nested_g
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()

        g = group(sig_1, sig_2, app=self.app)
        g_res = g.freeze()
        g.apply()

        with subtests.test("sig_1_res is stamped", groups=[g_res.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g_res.id]
        with subtests.test("sig_2_res is stamped", groups=nested_g_res._get_task_meta()['groups']):
            assert sig_2_res._get_task_meta()['groups'] == nested_g_res._get_task_meta()['groups']

    def test_group_stamping_three_levels(self, subtests):
        """
        For groups with three levels of nesting, test that group stamps
        are saved in the correct order for all nesting levels.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_in_g1_1 = self.add.s(2, 2)
        sig_in_g1_2 = self.add.s(1, 1)
        sig_in_g2 = self.add.s(2)
        sig_in_g2_chain = self.add.s(4)
        sig_in_g3_1 = self.add.s(8)
        sig_in_g3_2 = self.add.s(16)

        sig_in_g1_1_res = sig_in_g1_1.freeze()
        sig_in_g1_2_res = sig_in_g1_2.freeze()
        sig_in_g2_res = sig_in_g2.freeze()
        sig_in_g2_chain_res = sig_in_g2_chain.freeze()
        sig_in_g3_1_res = sig_in_g3_1.freeze()
        sig_in_g3_2_res = sig_in_g3_2.freeze()

        g3 = group(
            sig_in_g3_1,
            sig_in_g3_2,
            app=self.app
        )

        g3_res = g3.freeze()

        g2 = group(
            sig_in_g2,
            chain(
                sig_in_g2_chain,
                g3
            ),
            app=self.app
        )

        g2_res = g2.freeze()

        g1 = group(
            sig_in_g1_1,
            chain(
                sig_in_g1_2,
                g2,
                app=self.app
            ),
            app=self.app
        )

        g1_res = g1.freeze()
        g1.apply()

        with subtests.test("sig_in_g1_1_res is stamped", groups=[g1_res.id]):
            assert sig_in_g1_1_res._get_task_meta()['groups'] == [g1_res.id]
        with subtests.test("sig_in_g1_2_res is stamped", groups=[g1_res.id]):
            assert sig_in_g1_2_res._get_task_meta()['groups'] == [g1_res.id]
        with subtests.test("sig_in_g2_res is stamped", groups=[g1_res.id, g2_res.id]):
            assert sorted(sig_in_g2_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])
        with subtests.test("sig_in_g2_chain_res is stamped", groups=[g1_res.id, g2_res.id]):
            assert sorted(sig_in_g2_chain_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])
        with subtests.test("sig_in_g3_1_res is stamped", groups=[g1_res.id, g2_res.id, g3_res.id]):
            assert sorted(sig_in_g3_1_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id, g3_res.id])
        with subtests.test("sig_in_g3_2_res is stamped", groups=[g1_res.id, g2_res.id, g3_res.id]):
            assert sorted(sig_in_g3_2_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id, g3_res.id])

    def test_group_stamping_parallel_groups(self, subtests):
        """
        In the case of group within a group that is from another canvas
        element, ensure that group stamps are added correctly when groups are
        run in parallel.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_in_g1 = self.add.s(1, 1)
        sig_in_g2_chain = self.add.s(2, 2)
        sig_in_g2_1 = self.add.s(4)
        sig_in_g2_2 = self.add.s(8)
        sig_in_g3_chain = self.add.s(2, 2)
        sig_in_g3_1 = self.add.s(4)
        sig_in_g3_2 = self.add.s(8)

        sig_in_g1_res = sig_in_g1.freeze(_id='sig_in_g1')
        sig_in_g2_chain_res = sig_in_g2_chain.freeze(_id='sig_in_g2_chain')
        sig_in_g2_1_res = sig_in_g2_1.freeze(_id='sig_in_g2_1')
        sig_in_g2_2_res = sig_in_g2_2.freeze(_id='sig_in_g2_2')
        sig_in_g3_chain_res = sig_in_g3_chain.freeze(_id='sig_in_g3_chain')
        sig_in_g3_1_res = sig_in_g3_1.freeze(_id='sig_in_g3_1')
        sig_in_g3_2_res = sig_in_g3_2.freeze(_id='sig_in_g3_2')

        g3 = group(
            sig_in_g3_1,
            sig_in_g3_2,
            app=self.app
        )
        g3_res = g3.freeze(group_id='g3')

        g2 = group(
            sig_in_g2_1,
            sig_in_g2_2,
            app=self.app
        )
        g2_res = g2.freeze(group_id='g2')

        g1 = group(
            sig_in_g1,
            chain(
                sig_in_g2_chain,
                g2,
                app=self.app
            ),
            chain(
                sig_in_g3_chain,
                g3,
                app=self.app
            ),
        )
        g1_res = g1.freeze(group_id='g1')
        g1.apply()

        with subtests.test("sig_in_g1 is stamped", groups=[g1_res.id]):
            assert sig_in_g1_res.id == 'sig_in_g1'
            assert sig_in_g1_res._get_task_meta()['groups'] == [g1_res.id]

        with subtests.test("sig_in_g2_chain is stamped", groups=[g1_res.id]):
            assert sig_in_g2_chain_res.id == 'sig_in_g2_chain'
            assert sig_in_g2_chain_res._get_task_meta()['groups'] == \
                [g1_res.id]

        with subtests.test("sig_in_g2_1 is stamped", groups=[g1_res.id, g2_res.id]):
            assert sig_in_g2_1_res.id == 'sig_in_g2_1'
            assert sorted(sig_in_g2_1_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])

        with subtests.test("sig_in_g2_2 is stamped",
                           groups=[g1_res.id, g2_res.id]):
            assert sig_in_g2_2_res.id == 'sig_in_g2_2'
            assert sorted(sig_in_g2_2_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g2_res.id])

        with subtests.test("sig_in_g3_chain is stamped",
                           groups=[g1_res.id]):
            assert sig_in_g3_chain_res.id == 'sig_in_g3_chain'
            assert sig_in_g3_chain_res._get_task_meta()['groups'] == \
                [g1_res.id]

        with subtests.test("sig_in_g3_1 is stamped",
                           groups=[g1_res.id, g3_res.id]):
            assert sig_in_g3_1_res.id == 'sig_in_g3_1'
            assert sorted(sig_in_g3_1_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g3_res.id])

        with subtests.test("sig_in_g3_2 is stamped",
                           groups=[g1_res.id, g3_res.id]):
            assert sorted(sig_in_g3_2_res._get_task_meta()['groups']) == \
                sorted([g1_res.id, g3_res.id])

    def test_chord_stamping_one_level(self, subtests):
        """
        In the case of group within a chord that is from another canvas
        element, ensure that chord stamps are added correctly when chord are
        run in parallel.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(4, 4)
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()
        sig_sum = self.xsum.s()
        sig_sum_res = sig_sum.freeze()

        g = chord([sig_1, sig_2], sig_sum, app=self.app)
        g.stamp(stamp="stamp")
        g.freeze()
        g.apply()

        with subtests.test("sig_sum_res body isn't stamped", groups=[]):
            assert sig_sum_res._get_task_meta()['groups'] == []

        with subtests.test("sig_1_res is stamped", groups=[g.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g.id]

        with subtests.test("sig_2_res is stamped", groups=[g.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g.id]

        with subtests.test("sig_1_res is stamped manually", stamp=["stamp"]):
            assert sig_1_res._get_task_meta()['stamp'] == ["stamp"]

        with subtests.test("sig_2_res is stamped manually", stamp=["stamp"]):
            assert sig_2_res._get_task_meta()['stamp'] == ["stamp"]

        with subtests.test("sig_1_res has stamped_headers", stamped_headers=["stamp", 'groups']):
            assert sorted(sig_1_res._get_task_meta()['stamped_headers']) == sorted(['stamp', 'groups'])

        with subtests.test("sig_2_res has stamped_headers", stamped_headers=["stamp", 'groups']):
            assert sorted(sig_2_res._get_task_meta()['stamped_headers']) == sorted(['stamp', 'groups'])

    def test_chord_stamping_two_levels(self, subtests):
        """
        For a group within a chord, test that group stamps are stored in
        the correct order.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(1, 1)
        nested_sig_1 = self.add.s(2)
        nested_sig_2 = self.add.s(4)

        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()
        first_nested_sig_res = nested_sig_1.freeze()
        second_nested_sig_res = nested_sig_2.freeze()

        g2 = group(
            nested_sig_1,
            nested_sig_2,
            app=self.app
        )

        g2_res = g2.freeze()

        sig_sum = self.xsum.s()
        sig_sum.freeze()

        g1 = chord([sig_2, chain(sig_1, g2)], sig_sum, app=self.app)

        g1.freeze()
        g1.apply()

        with subtests.test("sig_1_res body is stamped", groups=[g1.id]):
            assert sig_1_res._get_task_meta()['groups'] == [g1.id]
        with subtests.test("sig_2_res body is stamped", groups=[g1.id]):
            assert sig_2_res._get_task_meta()['groups'] == [g1.id]
        with subtests.test("first_nested_sig_res body is stamped", groups=[g1.id, g2_res.id]):
            assert sorted(first_nested_sig_res._get_task_meta()['groups']) == \
                sorted([g1.id, g2_res.id])
        with subtests.test("second_nested_sig_res body is stamped", groups=[g1.id, g2_res.id]):
            assert sorted(second_nested_sig_res._get_task_meta()['groups']) == \
                sorted([g1.id, g2_res.id])

    def test_chord_stamping_body_group(self, subtests):
        """
        In the case of group within a chord that is from another canvas
        element, ensure that chord stamps are added correctly when chord are
        run in parallel.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        tasks = [self.add.s(i, i) for i in range(10)]

        sum_task = self.xsum.s()
        sum_task_res = sum_task.freeze()
        prod_task = self.xprod.s()
        prod_task_res = sum_task.freeze()

        body = group(sum_task, prod_task)

        g = chord(tasks, body, app=self.app)
        g.freeze()
        g.apply()

        with subtests.test("sum_task_res is stamped", groups=[body.id]):
            assert sum_task_res._get_task_meta()['groups'] == [body.id]
        with subtests.test("prod_task_res is stamped", groups=[body.id]):
            assert prod_task_res._get_task_meta()['groups'] == [body.id]

    def test_chord_stamping_body_chord(self, subtests):
        """
        In the case of chord within a chord that is from another canvas
        element, ensure that chord stamps are added correctly when chord are
        run in parallel.
        """
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        parent_header_tasks = group([self.add.s(i, i) for i in range(10)])
        parent_header_tasks_res = parent_header_tasks.freeze()

        sum_task = self.xsum.s()
        sum_task_res = sum_task.freeze()
        sum_task2 = self.xsum.s()
        sum_task_res2 = sum_task2.freeze()
        prod_task = self.xprod.s()
        prod_task_res = sum_task.freeze()

        body = chord(group(sum_task, prod_task), sum_task2, app=self.app)

        c = chord(parent_header_tasks, body, app=self.app)
        c.freeze()
        c.apply()

        with subtests.test("parent_header_tasks are stamped", groups=[c.id]):
            for ar in parent_header_tasks_res.children:
                assert ar._get_task_meta()['groups'] == [c.id]
                assert ar._get_task_meta()['groups'] != [body.id]
        with subtests.test("sum_task_res is stamped", groups=[body.id]):
            assert sum_task_res._get_task_meta()['groups'] == [body.id]
            assert sum_task_res._get_task_meta()['groups'] != [c.id]
        with subtests.test("prod_task_res is stamped", groups=[body.id]):
            assert prod_task_res._get_task_meta()['groups'] == [body.id]
            assert prod_task_res._get_task_meta()['groups'] != [c.id]
        with subtests.test("sum_task_res2 is NOT stamped", groups=[]):
            assert len(sum_task_res2._get_task_meta()['groups']) == 0
