from celery import chain, chord, group, workflow

from .tasks import (add, replace_with_chain_which_contains_a_group, replace_with_chord_of_chains,
                    replace_with_group_of_wf, tsum)

TIMEOUT = 60


class test_workflow:

    def test_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w = workflow()
        n1 = w.add_node(group([add.si(1, 1), add.si(2, 2)]))
        n2 = w.add_node(add.si(3, 3))
        n3 = w.add_node(tsum.s(), inputs={"nums": n1})
        n4 = w.add_node(add.s(), inputs={"x": n2, "y": n3})

        assert w.apply_async(outputs=[n2, n1, n4, n3]).get(timeout=TIMEOUT) == [6, [2, 4], 12, 6]

    def test_chain_ending_with_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w = workflow()
        n1 = w.add_node(chain(add.si(0, 0), group([add.si(1, 1), add.si(2, 2)])))
        n2 = w.add_node(add.si(3, 3))
        n3 = w.add_node(tsum.s(), inputs={"nums": n1})
        n4 = w.add_node(add.s(), inputs={"x": n2, "y": n3})

        assert w.apply_async(outputs=[n2, n1, n4, n3]).get(timeout=TIMEOUT) == [6, [2, 4], 12, 6]

    def test_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w = workflow()
        n1 = w.add_node(chain([add.si(1, 1), add.si(2, 2)]))
        n2 = w.add_node(chain([add.si(3, 3), add.si(4, 4)]))
        n3 = w.add_node(add.s(), inputs={"x": n1, "y": n2})

        assert w.apply_async(outputs=n3).get(timeout=TIMEOUT) == 12

    def test_chord_of_workflows(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w1 = workflow()
        n1 = w1.add_node(add.s(1, 1))
        w2 = workflow()
        n2 = w2.add_node(add.s(2, 2))
        w1.set_outputs(outputs=n1)
        w2.set_outputs(outputs=n2)
        result = chord([w1, w2], tsum.s()).delay()
        result = result.get(timeout=TIMEOUT)
        assert result == 6

    def test_chain_of_workflows(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w1 = workflow()
        n1 = w1.add_node(group([add.s(1, 1), add.s(2, 2)]))
        w2 = workflow()
        n2 = w2.add_node(tsum.s())
        w1.set_outputs(outputs=n1)
        w2.set_outputs(outputs=n2)
        result = chain([w1, w2]).delay()
        result = result.get(timeout=TIMEOUT)
        assert result == 6

    def test_workflow_in_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w = workflow()
        n1 = w.add_node(group([add.s(1), add.s(2)]))
        n2 = w.add_node(tsum.s(), inputs={"nums": n1})
        w.set_outputs(outputs=n2)
        result = chain([add.s(1, 1), w, add.s(3)]).delay()
        result = result.get(timeout=TIMEOUT)
        assert result == 10

    def test_node_replace_chord_of_chains(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w = workflow()
        w.add_node(replace_with_chord_of_chains.s())
        result = w.apply_async(outputs=w.nodes).get(timeout=TIMEOUT)
        assert result == [[0, 2, 4, 6, 8]]

    def test_replace_with_group_of_workflows(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        result = replace_with_group_of_wf.s().delay().get(timeout=TIMEOUT)
        assert list(sorted(result)) == [[0, 0, 0], [2, 2, 4], [4, 4, 8], [6, 6, 12], [8, 8, 16]]

    def test_replace_with_chain_which_contains_a_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w = workflow()
        n1 = w.add_node(replace_with_chain_which_contains_a_group.s())
        n2 = w.add_node(tsum.s(), inputs={"nums": n1})
        result = w.apply_async(outputs=n2).get(timeout=TIMEOUT)
        assert result == 8

    def test_outputs(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w = workflow()
        n1 = w.add_node(group([add.si(1, 1), add.si(2, 2)]))
        n2 = w.add_node(add.si(3, 3))
        n3 = w.add_node(tsum.s(), inputs={"nums": n1})
        n4 = w.add_node(add.s(), inputs={"x": n2, "y": n3})

        assert w.apply_async(outputs=[n2, n1, n4, n3]).get(timeout=TIMEOUT) == [6, [2, 4], 12, 6]

    def test_single_output(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w = workflow()
        n1 = w.add_node(group([add.si(1, 1), add.si(2, 2)]))
        n2 = w.add_node(add.si(3, 3))
        n3 = w.add_node(tsum.s(), inputs={"nums": n1})
        w.add_node(add.s(), inputs={"x": n2, "y": n3})

        assert w.apply_async(outputs=n1).get(timeout=TIMEOUT) == [2, 4]

    def test_single_output_list(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        w = workflow()
        n1 = w.add_node(group([add.si(1, 1), add.si(2, 2)]))
        n2 = w.add_node(add.si(3, 3))
        n3 = w.add_node(tsum.s(), inputs={"nums": n1})
        w.add_node(add.s(), inputs={"x": n2, "y": n3})

        assert w.apply_async(outputs=[n1]).get(timeout=TIMEOUT) == [[2, 4]]
