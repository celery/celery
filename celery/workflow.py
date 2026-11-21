from kombu.utils.uuid import uuid

from celery.canvas import Signature, _chain, _chord, maybe_signature
from celery.utils.objects import getitem_property

__all__ = (
    'workflow',
)


@Signature.register_type(name="workflow")
class workflow(Signature):
    tasks = getitem_property("kwargs.tasks", "Tasks in workflow.")
    output_idxs = getitem_property("kwargs.output_idxs", "Indexes into nodes for desired outputs.")
    g = getitem_property("kwargs.g", "Tasks graph.")

    def __init__(self, tasks=None, output_idxs=None, g=None, args=None, kwargs=None, app=None, **options):

        self.kwargs = {}

        self.tasks = [maybe_signature(task) for task in tasks] if tasks else []
        self.nodes = self.tasks
        self.outputs = None
        self.g = {int(k): v for k, v in g.items()} if g else {}
        self.output_idxs = output_idxs
        self.subtask_type = "workflow"

        super().__init__(
            "workflow", args=args, kwargs=self.kwargs, app=app, subtask_type="workflow", **options
        )

    @classmethod
    def from_dict(cls, d, app=None):
        return cls(
            *(d.get("args") or ()),
            app=app,
            **{**(d.get("kwargs") or {}), **(d.get("options") or {})},
        )

    def freeze(self, _id=None, group_id=None, chord=None, workflow=None,
               root_id=None, parent_id=None, group_index=None):
        self.prepare(_id)
        for node in self.nodes:
            node.set(group_id=group_id, root_id=root_id, parent_id=parent_id, group_index=group_index)
        self.nodes[-1].freeze()
        return self.AsyncResult(self.nodes[-1].id)

    def set_outputs(self, outputs=None):
        node_ids = [id(node) for node in self.nodes]
        if isinstance(outputs, list):
            self.output_idxs = [node_ids.index(id(output)) for output in outputs]
        elif outputs is not None:
            self.output_idxs = node_ids.index(id(outputs))
        else:
            self.output_idxs = list(range(len(self.nodes)))

    def prepare(self, _id=None, outputs=None, **options):
        if outputs is not None or self.output_idxs is None:
            self.set_outputs(outputs=outputs)
        self.nodes.append(self.app.tasks['celery.accumulate'].s(kwarg="arg"))
        if _id:
            self.nodes[-1].freeze(_id)
        self.nodes[-1].set(**options)
        self.g[len(self.nodes) - 1] = {"arg": self.output_idxs}
        kwarg_map = {}
        for node_idx, parents in self.g.items():
            parent_idxs = [
                idx
                for idxs in parents.values()
                for idx in (idxs if isinstance(idxs, list) else [idxs])
            ]
            parent_idxs = list(set(parent_idxs))
            node_sig = self.nodes[node_idx]
            node_sig.freeze()
            header_tasks = [self.nodes[parent_idx] for parent_idx in parent_idxs]
            for task in header_tasks:
                task.freeze()
            group_id = uuid()
            for idx, parent_idx in enumerate(parent_idxs):
                kwarg_map[parent_idx] = idx
                parent_sig = self.nodes[parent_idx]
                if isinstance(parent_sig, workflow):
                    output_sig = parent_sig.nodes[-1]
                elif isinstance(parent_sig, _chord):
                    output_sig = parent_sig.body
                elif isinstance(parent_sig, _chain):
                    output_sig = parent_sig.tasks[-1]
                else:
                    output_sig = parent_sig
                output_sig.set(chord=None)
                output_sig.options.setdefault("workflow", [])
                output_sig.options["workflow"].append(
                    {
                        "group": group_id,
                        "group_index": idx,
                        "chord": node_sig,
                        "parents": [
                            (
                                t.nodes[-1].id
                                if isinstance(t, workflow)
                                else t.body.id if isinstance(t, _chord)
                                else t.id
                            )
                            for t in header_tasks
                        ]
                    }
                )
            if not node_sig.immutable and any(p is not None for p in parents.keys()):
                kwarg_mapping = {
                    kwarg: (
                        kwarg_map[parent_idx]
                        if isinstance(parent_idx, int)
                        else [kwarg_map[pidx] for pidx in parent_idx]
                    )
                    for kwarg, parent_idx in parents.items()
                    if kwarg is not None
                }
                if header_tasks := getattr(node_sig, "tasks", None):
                    for header_sig in header_tasks.tasks:
                        header_sig.set(kwarg_mapping=kwarg_mapping)
                else:
                    node_sig.set(kwarg_mapping=kwarg_mapping)
            if parent_idxs:
                node_sig.app.backend.set_chord_size(group_id, len(parent_idxs))
        for node in self.nodes:
            node.set(workflow_output_id=self.nodes[-1].id)
        self.tasks = self.nodes
        return self

    def apply_async(self, args=None, kwargs=None, outputs=None, **options):

        options = {k: v for k, v in options.items() if k not in ("group_index", "producer")}

        self.prepare(outputs=outputs, **options)

        options = {k: v for k, v in options.items() if k not in ("chain", "chord", "group_id")}

        for idx, node in enumerate(self.nodes):
            if idx not in self.g:
                node.apply_async(args=args, kwargs=kwargs, **options)

        return self.AsyncResult(self.nodes[-1].id)

    def add_node(self, sig, inputs=None, depends_on=None, unpack_outputs=False):
        if sig.task == "celery.group":
            sig = _chord(sig.tasks, self.app.tasks['celery.accumulate'].s(index=0))
        elif isinstance(sig, _chain) and sig.tasks[-1].task == "celery.group":
            sig.tasks = list(sig.tasks)
            sig.tasks[-1] = _chord(sig.tasks[-1].tasks, self.app.tasks['celery.accumulate'].s(index=0))

        self.nodes.append(sig)
        node_ids = [id(node) for node in self.nodes]
        if inputs:
            sig.immutable = False
            self.g[len(self.nodes) - 1] = {
                kwarg: (
                    node_ids.index(id(parent_sig))
                    if not isinstance(parent_sig, list)
                    else [node_ids.index(id(psig)) for psig in parent_sig]
                )
                for kwarg, parent_sig in inputs.items()
            }
        if depends_on:
            self.g.setdefault(len(self.nodes) - 1, {})[None] = [
                node_ids.index(dep_id)
                for dep_id in {id(parent_sig) for parent_sig in depends_on}
            ]
        if unpack_outputs:
            sig.set(unpack_outputs=True)
        return sig

    def stamp(self, visitor=None, append_stamps=False, **headers):
        for node in self.nodes:
            node.stamp(visitor=visitor, append_stamps=append_stamps, **headers)
        return super().stamp(visitor=visitor, append_stamps=append_stamps, **headers)

    def set(self, immutable=None, **options):
        """Set arbitrary execution options (same as ``.options.update(…)``).

        Returns:
            Signature: This is a chaining method call
                (i.e., it will return ``self``).
        """
        if immutable is not None:
            self.set_immutable(immutable)
        self.nodes[-1].options.update(options)
        return self

    def set_immutable(self, immutable):
        for idx, node in enumerate(self.nodes):
            if idx not in self.g:
                node.set_immutable(immutable)
