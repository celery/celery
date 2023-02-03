import math
from collections.abc import Iterable

import pytest

from celery import Task
from celery.canvas import StampingVisitor, chain, chord, group, signature
from celery.exceptions import Ignore


class CustomStampingVisitor(StampingVisitor):
    # Should be applied on all signatures
    def on_signature(self, actual_sig, **headers) -> dict:
        return {"on_signature": True}

    # Should be applied on all groups
    def on_group_start(self, group, **headers) -> dict:
        return {"on_group_start": True}

    # Should be applied on all chains
    def on_chain_start(self, chain, **headers) -> dict:
        return {"on_chain_start": True}

    # Should be applied on all chords
    def on_chord_header_start(self, chord, **header) -> dict:
        s = super().on_chord_header_start(chord, **header)
        s.update({"on_chord_header_start": True})
        return s

    # Should be applied on all chords
    def on_chord_body(self, chord, **header) -> dict:
        return {"on_chord_body": True}

    # Should be applied on all links
    def on_callback(self, callback, **header) -> dict:
        return {"on_callback": True}

    # Should be applied on all link errors
    def on_errback(self, errback, **header) -> dict:
        return {"on_errback": True}


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


@pytest.mark.parametrize(
    "canvas_workflow",
    [
        signature("sig"),
        group(signature("sig1")),
        group(signature("sig1"), signature("sig2")),
        group(signature(f"sig{i}") for i in range(2)),
        group([signature("sig1"), signature("sig2")]),
        group((signature("sig1"), signature("sig2"))),
        chord([signature("sig1"), signature("sig2")], signature("sig3")),
        chord((signature("sig1"), signature("sig2")), signature("sig3")),
        chord(group(signature("sig1"), signature("sig2")), signature("sig3")),
        chord(group([signature("sig1"), signature("sig2")]), signature("sig3")),
        chord(group((signature("sig1"), signature("sig2"))), signature("sig3")),
        chain(signature("sig1"), signature("sig2")),
        chain([signature("sig1"), signature("sig2")]),
        chain((signature("sig1"), signature("sig2"))),
        chain(group(signature("sig1"), signature("sig2"))),
        chain(group([signature("sig1"), signature("sig2")])),
        chain(group((signature("sig1"), signature("sig2")))),
        chain(group(signature("sig1"), signature("sig2")), signature("sig3")),
        chain(group([signature("sig1"), signature("sig2")]), signature("sig3")),
        chain(group((signature("sig1"), signature("sig2"))), signature("sig3")),
        chain(signature("sig1"), group(signature("sig2"), signature("sig3"))),
        chain(signature("sig1"), group([signature("sig2"), signature("sig3")])),
        chain(signature("sig1"), group((signature("sig2"), signature("sig3")))),
        chain(
            group(signature("sig1"), signature("sig2")),
            group(signature("sig3"), signature("sig4")),
        ),
        chain(
            group([signature("sig1"), signature("sig2")]),
            group([signature("sig3"), signature("sig4")]),
        ),
        chain(
            group((signature("sig1"), signature("sig2"))),
            group((signature("sig3"), signature("sig4"))),
        ),
        chain(
            group(signature("sig1"), signature("sig2")),
            group([signature("sig3"), signature("sig4")]),
        ),
        chain(
            group([signature("sig1"), signature("sig2")]),
            group((signature("sig3"), signature("sig4"))),
        ),
        chain(
            group((signature("sig1"), signature("sig2"))),
            group(signature("sig3"), signature("sig4")),
        ),
        chain(
            group(signature("sig1"), signature("sig2")),
            group(signature("sig3"), signature("sig4")),
            signature("sig5"),
        ),
        chain(
            group([signature("sig1"), signature("sig2")]),
            group([signature("sig3"), signature("sig4")]),
            signature("sig5"),
        ),
        chain(
            group((signature("sig1"), signature("sig2"))),
            group((signature("sig3"), signature("sig4"))),
            signature("sig5"),
        ),
        chain(
            group(signature("sig1"), signature("sig2")),
            group([signature("sig3"), signature("sig4")]),
            signature("sig5"),
        ),
        chain(
            group([signature("sig1"), signature("sig2")]),
            group((signature("sig3"), signature("sig4"))),
            signature("sig5"),
        ),
        chain(
            group((signature("sig1"), signature("sig2"))),
            group(signature("sig3"), signature("sig4")),
            signature("sig5"),
        ),
        chord(
            group(signature("sig1"), signature("sig2")),
            group(signature("sig3"), signature("sig4")),
        ),
        chord(
            group([signature("sig1"), signature("sig2")]),
            group([signature("sig3"), signature("sig4")]),
        ),
        chord(
            group((signature("sig1"), signature("sig2"))),
            group((signature("sig3"), signature("sig4"))),
        ),
        chord(
            group(signature("sig1"), signature("sig2")),
            group([signature("sig3"), signature("sig4")]),
        ),
        chord(
            group([signature("sig1"), signature("sig2")]),
            group((signature("sig3"), signature("sig4"))),
        ),
        chord(
            group((signature("sig1"), signature("sig2"))),
            group(signature("sig3"), signature("sig4")),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group(signature("sig3"), signature("sig4")),
            ),
            signature("sig5"),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group([signature("sig3"), signature("sig4")]),
            ),
            signature("sig5"),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group((signature("sig3"), signature("sig4"))),
            ),
            signature("sig5"),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group([signature("sig3"), signature("sig4")]),
            ),
            signature("sig5"),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group((signature("sig3"), signature("sig4"))),
            ),
            signature("sig5"),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group(signature("sig3"), signature("sig4")),
            ),
            signature("sig5"),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group(signature("sig3"), signature("sig4")),
            ),
            group(signature("sig5"), signature("sig6")),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group([signature("sig3"), signature("sig4")]),
            ),
            group([signature("sig5"), signature("sig6")]),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group((signature("sig3"), signature("sig4"))),
            ),
            group((signature("sig5"), signature("sig6"))),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group([signature("sig3"), signature("sig4")]),
            ),
            group(signature("sig5"), signature("sig6")),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group((signature("sig3"), signature("sig4"))),
            ),
            group([signature("sig5"), signature("sig6")]),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group(signature("sig3"), signature("sig4")),
            ),
            group((signature("sig5"), signature("sig6"))),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group(signature("sig3"), signature("sig4")),
            ),
            chain(
                group(signature("sig5"), signature("sig6")),
                group(signature("sig7"), signature("sig8")),
            ),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group([signature("sig3"), signature("sig4")]),
            ),
            chain(
                group([signature("sig5"), signature("sig6")]),
                group([signature("sig7"), signature("sig8")]),
            ),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group((signature("sig3"), signature("sig4"))),
            ),
            chain(
                group((signature("sig5"), signature("sig6"))),
                group((signature("sig7"), signature("sig8"))),
            ),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group([signature("sig3"), signature("sig4")]),
            ),
            chain(
                group(signature("sig5"), signature("sig6")),
                group([signature("sig7"), signature("sig8")]),
            ),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group((signature("sig3"), signature("sig4"))),
            ),
            chain(
                group([signature("sig5"), signature("sig6")]),
                group((signature("sig7"), signature("sig8"))),
            ),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group(signature("sig3"), signature("sig4")),
            ),
            chain(
                group((signature("sig5"), signature("sig6"))),
                group(signature("sig7"), signature("sig8")),
            ),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group(signature("sig3"), signature("sig4")),
            ),
            chord(
                chain(
                    group(signature("sig5"), signature("sig6")),
                    group(signature("sig7"), signature("sig8")),
                ),
                signature("sig9"),
            ),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group([signature("sig3"), signature("sig4")]),
            ),
            chord(
                chain(
                    group([signature("sig5"), signature("sig6")]),
                    group([signature("sig7"), signature("sig8")]),
                ),
                signature("sig9"),
            ),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group((signature("sig3"), signature("sig4"))),
            ),
            chord(
                chain(
                    group((signature("sig5"), signature("sig6"))),
                    group((signature("sig7"), signature("sig8"))),
                ),
                signature("sig9"),
            ),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group([signature("sig3"), signature("sig4")]),
            ),
            chord(
                chain(
                    group(signature("sig5"), signature("sig6")),
                    group([signature("sig7"), signature("sig8")]),
                ),
                signature("sig9"),
            ),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group((signature("sig3"), signature("sig4"))),
            ),
            chord(
                chain(
                    group([signature("sig5"), signature("sig6")]),
                    group((signature("sig7"), signature("sig8"))),
                ),
                signature("sig9"),
            ),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group(signature("sig3"), signature("sig4")),
            ),
            chord(
                chain(
                    group((signature("sig5"), signature("sig6"))),
                    group(signature("sig7"), signature("sig8")),
                ),
                signature("sig9"),
            ),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group(signature("sig3"), signature("sig4")),
            ),
            chain(
                chord(
                    chain(
                        group(signature("sig5"), signature("sig6")),
                        group(signature("sig7"), signature("sig8")),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group([signature("sig3"), signature("sig4")]),
            ),
            chain(
                chord(
                    chain(
                        group([signature("sig5"), signature("sig6")]),
                        group([signature("sig7"), signature("sig8")]),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group((signature("sig3"), signature("sig4"))),
            ),
            chain(
                chord(
                    chain(
                        group((signature("sig5"), signature("sig6"))),
                        group((signature("sig7"), signature("sig8"))),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group([signature("sig3"), signature("sig4")]),
            ),
            chain(
                chord(
                    chain(
                        group(signature("sig5"), signature("sig6")),
                        group([signature("sig7"), signature("sig8")]),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group((signature("sig3"), signature("sig4"))),
            ),
            chain(
                chord(
                    chain(
                        group([signature("sig5"), signature("sig6")]),
                        group((signature("sig7"), signature("sig8"))),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group(signature("sig3"), signature("sig4")),
            ),
            chain(
                chord(
                    chain(
                        group((signature("sig5"), signature("sig6"))),
                        group(signature("sig7"), signature("sig8")),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group(signature("sig3"), signature("sig4")),
            ),
            chain(
                chord(
                    chain(
                        group(signature("sig5"), signature("sig6")),
                        group(signature("sig7"), signature("sig8")),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group([signature("sig3"), signature("sig4")]),
            ),
            chain(
                chord(
                    chain(
                        group([signature("sig5"), signature("sig6")]),
                        group([signature("sig7"), signature("sig8")]),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group((signature("sig3"), signature("sig4"))),
            ),
            chain(
                chord(
                    chain(
                        group((signature("sig5"), signature("sig6"))),
                        group((signature("sig7"), signature("sig8"))),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group([signature("sig3"), signature("sig4")]),
            ),
            chain(
                chord(
                    chain(
                        group(signature("sig5"), signature("sig6")),
                        group([signature("sig7"), signature("sig8")]),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group((signature("sig3"), signature("sig4"))),
            ),
            chain(
                chord(
                    chain(
                        group([signature("sig5"), signature("sig6")]),
                        group((signature("sig7"), signature("sig8"))),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group(signature("sig3"), signature("sig4")),
            ),
            chain(
                chord(
                    chain(
                        group((signature("sig5"), signature("sig6"))),
                        group(signature("sig7"), signature("sig8")),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group(signature("sig1"), signature("sig2")),
                group(signature("sig3"), signature("sig4")),
            ),
            chain(
                chord(
                    chain(
                        group(signature("sig5"), signature("sig6")),
                        group(signature("sig7"), signature("sig8")),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group([signature("sig1"), signature("sig2")]),
                group([signature("sig3"), signature("sig4")]),
            ),
            chain(
                chord(
                    chain(
                        group([signature("sig5"), signature("sig6")]),
                        group([signature("sig7"), signature("sig8")]),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group((signature("sig1"), signature("sig2"))),
                group(signature(f"sig{i}") for i in range(3, 5)),
            ),
            chain(
                chord(
                    chain(
                        group(signature(f"sig{i}") for i in range(5, 7)),
                        group(signature(f"sig{i}") for i in range(7, 9)),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
        chord(
            chain(
                group(signature(f"sig{i}") for i in range(1, 3)),
                group(signature(f"sig{i}") for i in range(3, 5)),
            ),
            chain(
                chord(
                    chain(
                        group(signature(f"sig{i}") for i in range(5, 7)),
                        group(signature(f"sig{i}") for i in range(7, 9)),
                    ),
                    signature("sig9"),
                ),
                signature("sig10"),
            ),
        ),
    ],
)
class test_canvas_stamping(CanvasCase):
    @pytest.fixture
    def stamped_sig(self, canvas_workflow):
        canvas_workflow.stamp(CustomStampingVisitor())
        return canvas_workflow

    def test_stamp_in_options(self, stamped_sig, subtests):
        """Test that all canvas signatures gets the stamp in options"""

        class AssersionVisitor(StampingVisitor):
            """
            The canvas stamping mechanism traverses the canvas automatically, so we can ride
            it to traverse the canvas recursively and assert that all signatures have the correct stamp in options
            """

            def on_signature(self, actual_sig, **headers) -> dict:
                with subtests.test(f"Check if {actual_sig.name} has on_signature stamp"):
                    assert actual_sig.options["on_signature"] is True, f"{actual_sig.name} has no on_signature stamp"
                return {}

            def on_group_start(self, group, **headers) -> dict:
                with subtests.test(f"Check if {group.name} has on_group_start stamp"):
                    assert group.options["on_group_start"] is True, f"{group.name} has no on_group_start stamp"
                return super().on_group_start(group, **headers)

            def on_chain_start(self, chain, **headers) -> dict:
                with subtests.test(f"Check if {chain.name} has on_chain_start stamp"):
                    assert chain.options["on_chain_start"] is True, f"{chain.name} has no on_chain_start stamp"
                return super().on_chain_start(chain, **headers)

            def on_chord_header_start(self, chord, **header) -> dict:
                with subtests.test(f"Check if {chord.name} has on_chord_header_start stamp"):
                    assert (
                        chord.options["on_chord_header_start"] is True
                    ), f"{chord.name} has no on_chord_header_start stamp"

                with subtests.test(f"Check if {chord.tasks.name} has on_chord_header_start stamp"):
                    assert (
                        chord.tasks.options["on_chord_header_start"] is True
                    ), f"{chord.tasks.name} has no on_chord_header_start stamp"
                return super().on_chord_header_start(chord, **header)

            def on_chord_body(self, chord, **header) -> dict:
                with subtests.test(f"Check if {chord.body.name} has on_chord_body stamp"):
                    assert (
                        chord.body.options["on_chord_body"] is True
                    ), f"{chord.body.name} has no on_chord_body stamp"
                return super().on_chord_body(chord, **header)

            def on_callback(self, callback, **header) -> dict:
                with subtests.test(f"Check if {callback.name} has on_callback stamp"):
                    assert callback.options["on_callback"] is True, f"{callback.name} has no on_callback stamp"
                return super().on_callback(callback, **header)

            def on_errback(self, errback, **header) -> dict:
                with subtests.test(f"Check if {errback.name} has on_errback stamp"):
                    assert errback.options["on_errback"] is True, f"{errback.name} has no on_errback stamp"
                return super().on_errback(errback, **header)

        stamped_sig.stamp(AssersionVisitor())

    def test_stamping_headers_in_options(self, stamped_sig, subtests):
        """Test that all canvas signatures gets the stamp in options["stamped_headers"]"""

        class AssersionVisitor(StampingVisitor):
            """
            The canvas stamping mechanism traverses the canvas automatically, so we can ride
            it to traverse the canvas recursively and assert that all signatures have the correct
            stamp in options["stamped_headers"]
            """

            def on_signature(self, actual_sig, **headers) -> dict:
                with subtests.test(f"Check if {actual_sig.name} has on_signature in stamped_headers"):
                    assert (
                        "on_signature" in actual_sig.options["stamped_headers"]
                    ), f"{actual_sig.name} has no on_signature in stamped_headers"
                return {}

            def on_group_start(self, group, **headers) -> dict:
                with subtests.test(f"Check if {group.name} has on_group_start in stamped_headers"):
                    assert (
                        "on_group_start" in group.options["stamped_headers"]
                    ), f"{group.name} has no on_group_start in stamped_headers"
                return super().on_group_start(group, **headers)

            def on_chain_start(self, chain, **headers) -> dict:
                with subtests.test(f"Check if {chain.name} has on_chain_start in stamped_headers"):
                    assert (
                        "on_chain_start" in chain.options["stamped_headers"]
                    ), f"{chain.name} has no on_chain_start in stamped_headers"
                return super().on_chain_start(chain, **headers)

            def on_chord_header_start(self, chord, **header) -> dict:
                with subtests.test(f"Check if {chord.name} has on_chord_header_start in stamped_headers"):
                    assert (
                        "on_chord_header_start" in chord.options["stamped_headers"]
                    ), f"{chord.name} has no on_chord_header_start in stamped_headers"

                with subtests.test(f"Check if {chord.tasks.name} has on_chord_header_start in stamped_headers"):
                    assert (
                        "on_chord_header_start" in chord.tasks.options["stamped_headers"]
                    ), f"{chord.tasks.name} has no on_chord_header_start in stamped_headers"
                return super().on_chord_header_start(chord, **header)

            def on_chord_body(self, chord, **header) -> dict:
                with subtests.test(f"Check if {chord.body.name} has on_chord_body in stamped_headers"):
                    assert (
                        "on_chord_body" in chord.body.options["stamped_headers"]
                    ), f"{chord.body.name} has no on_chord_body in stamped_headers"
                return super().on_chord_body(chord, **header)

            def on_callback(self, callback, **header) -> dict:
                with subtests.test(f"Check if {callback.name} has on_callback in stamped_headers"):
                    assert (
                        "on_callback" in callback.options["stamped_headers"]
                    ), f"{callback.name} has no on_callback in stamped_headers"
                return super().on_callback(callback, **header)

            def on_errback(self, errback, **header) -> dict:
                with subtests.test(f"Check if {errback.name} has on_errback in stamped_headers"):
                    assert (
                        "on_errback" in errback.options["stamped_headers"]
                    ), f"{errback.name} has no on_errback in stamped_headers"
                return super().on_errback(errback, **header)

        stamped_sig.stamp(AssersionVisitor())


class test_stamping_mechanism(CanvasCase):
    """These tests were extracted (and fixed) from the canvas unit tests."""

    def test_on_signature_gets_the_signature(self):
        expected_sig = self.add.s(4, 2)

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, actual_sig, **headers) -> dict:
                nonlocal expected_sig
                assert actual_sig == expected_sig
                return {"header": "value"}

        sig = expected_sig.clone()
        sig.stamp(CustomStampingVisitor())
        assert sig.options["header"] == "value"

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
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["stamp2", "stamp1"])

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
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["stamp"])

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
        assert sorted(sig_1_res._get_task_meta()["groups"]) == sorted(stamps)

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
                return {"header": "value"}

        class CustomStampingVisitor2(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"header": "value", "stamped_headers": ["header"]}

        sig_1 = self.add.s(2, 2)
        sig_1.stamp(visitor=CustomStampingVisitor1())
        sig_1_res = sig_1.freeze()
        sig_1.apply()
        sig_2 = self.add.s(2, 2)
        sig_2.stamp(visitor=CustomStampingVisitor2())
        sig_2_res = sig_2.freeze()
        sig_2.apply()

        with subtests.test("sig_1 is stamped with custom visitor", stamped_headers=["header"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["header"])

        with subtests.test("sig_2 is stamped with custom visitor", stamped_headers=["header"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["header"])

        with subtests.test("sig_1 is stamped with custom visitor", header=["value"]):
            assert sig_1_res._get_task_meta()["header"] == ["value"]

        with subtests.test("sig_2 is stamped with custom visitor", header=["value"]):
            assert sig_2_res._get_task_meta()["header"] == ["value"]

    def test_callback_stamping(self, subtests):
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"header": "value"}

            def on_callback(self, callback, **header) -> dict:
                return {"on_callback": True}

            def on_errback(self, errback, **header) -> dict:
                return {"on_errback": True}

        sig_1 = self.add.s(0, 1)
        sig_1_res = sig_1.freeze()
        group_sig = group([self.add.s(3), self.add.s(4)])
        group_sig_res = group_sig.freeze()
        chord_sig = chord([self.xsum.s(), self.xsum.s()], self.xsum.s())
        chord_sig_res = chord_sig.freeze()
        sig_2 = self.add.s(2)
        sig_2_res = sig_2.freeze()
        chain_sig = chain(
            sig_1,  # --> 1
            group_sig,  # --> [1+3, 1+4] --> [4, 5]
            chord_sig,  # --> [4+5, 4+5] --> [9, 9] --> 9+9 --> 18
            sig_2,  # --> 18 + 2 --> 20
        )
        callback = signature("callback_task")
        errback = signature("errback_task")
        chain_sig.link(callback)
        chain_sig.link_error(errback)
        chain_sig.stamp(visitor=CustomStampingVisitor())
        chain_sig_res = chain_sig.apply_async()
        chain_sig_res.get()

        with subtests.test("Confirm the chain was executed correctly", result=20):
            # Before we run our assersions, let's confirm the base functionality of the chain is working
            # as expected including the links stamping.
            assert chain_sig_res.result == 20

        with subtests.test("sig_1 is stamped with custom visitor", stamped_headers=["header"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["header"])

        with subtests.test("group_sig is stamped with custom visitor", stamped_headers=["header"]):
            for result in group_sig_res.results:
                assert sorted(result._get_task_meta()["stamped_headers"]) == sorted(["header"])

        with subtests.test("chord_sig is stamped with custom visitor", stamped_headers=["header"]):
            assert sorted(chord_sig_res._get_task_meta()["stamped_headers"]) == sorted(["header"])

        with subtests.test("sig_2 is stamped with custom visitor", stamped_headers=["header"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["header"])

        with subtests.test(
            "callback is stamped with custom visitor",
            stamped_headers=["header", "on_callback"],
        ):
            callback_link = chain_sig.options["link"][0]
            headers = callback_link.options
            stamped_headers = headers["stamped_headers"]
            assert sorted(stamped_headers) == sorted(["header", "on_callback"])
            assert headers["on_callback"] is True
            assert headers["header"] == "value"

        with subtests.test(
            "errback is stamped with custom visitor",
            stamped_headers=["header", "on_errback"],
        ):
            errback_link = chain_sig.options["link_error"][0]
            headers = errback_link.options
            stamped_headers = headers["stamped_headers"]
            assert sorted(stamped_headers) == sorted(["header", "on_errback"])
            assert headers["on_errback"] is True
            assert headers["header"] == "value"

    def test_callback_stamping_link_after_stamp(self, subtests):
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"header": "value"}

            def on_callback(self, callback, **header) -> dict:
                return {"on_callback": True}

            def on_errback(self, errback, **header) -> dict:
                return {"on_errback": True}

        class MyStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"stamp": "stamp"}

        sig_1 = self.add.s(0, 1)
        sig_1_res = sig_1.freeze()
        group_sig = group([self.add.s(3), self.add.s(4)])
        group_sig_res = group_sig.freeze()
        chord_sig = chord([self.xsum.s(), self.xsum.s()], self.xsum.s())
        chord_sig_res = chord_sig.freeze()
        sig_2 = self.add.s(2)
        sig_2_res = sig_2.freeze()
        chain_sig = chain(
            sig_1,  # --> 1
            group_sig,  # --> [1+3, 1+4] --> [4, 5]
            chord_sig,  # --> [4+5, 4+5] --> [9, 9] --> 9+9 --> 18
            sig_2,  # --> 18 + 2 --> 20
        )
        callback = signature("callback_task")
        errback = signature("errback_task")
        chain_sig.stamp(visitor=CustomStampingVisitor())
        chain_sig.link(callback)
        chain_sig.link_error(errback)
        chain_sig.stamp(visitor=MyStampingVisitor())
        chain_sig_res = chain_sig.apply_async()
        chain_sig_res.get()

        with subtests.test("Confirm the chain was executed correctly", result=20):
            # Before we run our assersions, let's confirm the base functionality of the chain is working
            # as expected including the links stamping.
            assert chain_sig_res.result == 20

        with subtests.test("sig_1 is stamped with custom visitor", stamped_headers=["header", "stamp"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["header", "stamp"])

        with subtests.test(
            "group_sig is stamped with custom visitor",
            stamped_headers=["header", "stamp"],
        ):
            for result in group_sig_res.results:
                assert sorted(result._get_task_meta()["stamped_headers"]) == sorted(["header", "stamp"])

        with subtests.test(
            "chord_sig is stamped with custom visitor",
            stamped_headers=["header", "stamp"],
        ):
            assert sorted(chord_sig_res._get_task_meta()["stamped_headers"]) == sorted(["header", "stamp"])

        with subtests.test("sig_2 is stamped with custom visitor", stamped_headers=["header", "stamp"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["header", "stamp"])

        with subtests.test("callback is stamped with MyStampingVisitor", stamped_headers=["stamp"]):
            callback_link = chain_sig.options["link"][0]
            headers = callback_link.options
            stamped_headers = headers["stamped_headers"]
            assert "on_callback" not in stamped_headers, "Linking after stamping should not stamp the callback"
            assert sorted(stamped_headers) == sorted(["stamp"])
            assert headers["stamp"] == "stamp"

        with subtests.test("errback is stamped with MyStampingVisitor", stamped_headers=["stamp"]):
            errback_link = chain_sig.options["link_error"][0]
            headers = errback_link.options
            stamped_headers = headers["stamped_headers"]
            assert "on_callback" not in stamped_headers, "Linking after stamping should not stamp the errback"
            assert sorted(stamped_headers) == sorted(["stamp"])
            assert headers["stamp"] == "stamp"

    @pytest.mark.usefixtures("depends_on_current_app")
    def test_callback_stamping_on_replace(self, subtests):
        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"header": "value"}

            def on_callback(self, callback, **header) -> dict:
                return {"on_callback": True}

            def on_errback(self, errback, **header) -> dict:
                return {"on_errback": True}

        class MyTask(Task):
            def on_replace(self, sig):
                sig.stamp(CustomStampingVisitor())
                return super().on_replace(sig)

        mytask = self.app.task(shared=False, base=MyTask)(return_True)

        sig1 = signature("sig1")
        callback = signature("callback_task")
        errback = signature("errback_task")
        sig1.link(callback)
        sig1.link_error(errback)

        with subtests.test("callback is not stamped with custom visitor yet"):
            callback_link = sig1.options["link"][0]
            headers = callback_link.options
            assert "on_callback" not in headers
            assert "header" not in headers

        with subtests.test("errback is not stamped with custom visitor yet"):
            errback_link = sig1.options["link_error"][0]
            headers = errback_link.options
            assert "on_errback" not in headers
            assert "header" not in headers

        with pytest.raises(Ignore):
            mytask.replace(sig1)

        with subtests.test(
            "callback is stamped with custom visitor",
            stamped_headers=["header", "on_callback"],
        ):
            callback_link = sig1.options["link"][0]
            headers = callback_link.options
            stamped_headers = headers["stamped_headers"]
            assert sorted(stamped_headers) == sorted(["header", "on_callback"])
            assert headers["on_callback"] is True
            assert headers["header"] == "value"

        with subtests.test(
            "errback is stamped with custom visitor",
            stamped_headers=["header", "on_errback"],
        ):
            errback_link = sig1.options["link_error"][0]
            headers = errback_link.options
            stamped_headers = headers["stamped_headers"]
            assert sorted(stamped_headers) == sorted(["header", "on_errback"])
            assert headers["on_errback"] is True
            assert headers["header"] == "value"

    @pytest.mark.parametrize(
        "sig_to_replace",
        [
            group(signature(f"sig{i}") for i in range(2)),
            group([signature("sig1"), signature("sig2")]),
            group((signature("sig1"), signature("sig2"))),
            group(signature("sig1"), signature("sig2")),
            chain(signature("sig1"), signature("sig2")),
        ],
    )
    @pytest.mark.usefixtures("depends_on_current_app")
    def test_replacing_stamped_canvas_with_tasks(self, subtests, sig_to_replace):
        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"header": "value"}

        class MyTask(Task):
            def on_replace(self, sig):
                nonlocal assertion_result
                nonlocal failed_task
                tasks = sig.tasks.tasks if isinstance(sig.tasks, group) else sig.tasks
                assertion_result = len(tasks) == 2
                for task in tasks:
                    assertion_result = all(
                        [
                            assertion_result,
                            "header" in task.options["stamped_headers"],
                            all([header in task.options for header in task.options["stamped_headers"]]),
                        ]
                    )
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

    @pytest.mark.usefixtures("depends_on_current_app")
    def test_replacing_stamped_canvas_with_tasks_with_links(self):
        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"header": "value"}

        class MyTask(Task):
            def on_replace(self, sig):
                nonlocal assertion_result
                nonlocal failed_task
                nonlocal failed_task_link
                tasks = sig.tasks.tasks if isinstance(sig.tasks, group) else sig.tasks
                assertion_result = True
                for task in tasks:
                    links = task.options["link"]
                    links.extend(task.options["link_error"])
                    for link in links:
                        assertion_result = all(
                            [
                                assertion_result,
                                all(
                                    [
                                        stamped_header in link["options"]
                                        for stamped_header in link["options"]["stamped_headers"]
                                    ]
                                ),
                            ]
                        )
                    else:
                        if not assertion_result:
                            failed_task_link = link
                            break

                    assertion_result = all(
                        [
                            assertion_result,
                            task.options["stamped_headers"]["header"] == "value",
                            all([header in task.options for header in task.options["stamped_headers"]]),
                        ]
                    )

                    if not assertion_result:
                        failed_task = task
                        break

                return super().on_replace(sig)

        @self.app.task(shared=False, bind=True, base=MyTask)
        def replace_from_MyTask(self):
            # Allows easy assertion for the test without using Mock
            return self.replace(sig_to_replace)

        s1 = chain(signature("foo11"), signature("foo12"))
        s1.link(signature("link_foo1"))
        s1.link_error(signature("link_error_foo1"))

        s2 = chain(signature("foo21"), signature("foo22"))
        s2.link(signature("link_foo2"))
        s2.link_error(signature("link_error_foo2"))

        sig_to_replace = group([s1, s2])
        sig = replace_from_MyTask.s()
        sig.stamp(CustomStampingVisitor())
        assertion_result = False
        failed_task = None
        failed_task_link = None
        sig.apply()

        err_msg = (
            f"Task {failed_task} was not stamped correctly"
            if failed_task
            else f"Task link {failed_task_link} was not stamped correctly"
            if failed_task_link
            else "Assertion failed"
        )
        assert assertion_result, err_msg

    def test_group_stamping_one_level(self, subtests):
        self.app.conf.task_always_eager = True
        self.app.conf.task_store_eager_result = True
        self.app.conf.result_extended = True

        sig_1 = self.add.s(2, 2)
        sig_2 = self.add.s(4, 4)
        sig_1_res = sig_1.freeze()
        sig_2_res = sig_2.freeze()

        g = group(sig_1, sig_2, app=self.app)
        g.stamp(stamp="stamp")
        g.apply()

        with subtests.test("sig_1_res is stamped manually", stamp=["stamp"]):
            assert sig_1_res._get_task_meta()["stamp"] == ["stamp"]

        with subtests.test("sig_2_res is stamped manually", stamp=["stamp"]):
            assert sig_2_res._get_task_meta()["stamp"] == ["stamp"]

        with subtests.test("sig_1_res has stamped_headers", stamped_headers=["stamp"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["stamp"])

        with subtests.test("sig_2_res has stamped_headers", stamped_headers=["stamp"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["stamp"])

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

        g = chord([sig_1, sig_2], sig_sum, app=self.app)
        g.stamp(stamp="stamp")
        g.freeze()
        g.apply()

        with subtests.test("sig_1_res is stamped manually", stamp=["stamp"]):
            assert sig_1_res._get_task_meta()["stamp"] == ["stamp"]

        with subtests.test("sig_2_res is stamped manually", stamp=["stamp"]):
            assert sig_2_res._get_task_meta()["stamp"] == ["stamp"]

        with subtests.test("sig_1_res has stamped_headers", stamped_headers=["stamp"]):
            assert sorted(sig_1_res._get_task_meta()["stamped_headers"]) == sorted(["stamp"])

        with subtests.test("sig_2_res has stamped_headers", stamped_headers=["stamp"]):
            assert sorted(sig_2_res._get_task_meta()["stamped_headers"]) == sorted(["stamp"])
