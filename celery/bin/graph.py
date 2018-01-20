# -*- coding: utf-8 -*-
"""The :program:`celery graph` command.

.. program:: celery graph
"""
from __future__ import absolute_import, unicode_literals

from operator import itemgetter

from celery.five import items, python_2_unicode_compatible
from celery.utils.graph import DependencyGraph, GraphFormatter

from .base import Command

__all__ = ('graph',)


class graph(Command):
    """The ``celery graph`` command."""

    args = """<TYPE> [arguments]
            .....  bootsteps [worker] [consumer]
            .....  workers   [enumerate]
    """

    def run(self, what=None, *args, **kwargs):
        map = {'bootsteps': self.bootsteps, 'workers': self.workers}
        if not what:
            raise self.UsageError('missing type')
        elif what not in map:
            raise self.Error('no graph {0} in {1}'.format(what, '|'.join(map)))
        return map[what](*args, **kwargs)

    def bootsteps(self, *args, **kwargs):
        worker = self.app.WorkController()
        include = {arg.lower() for arg in args or ['worker', 'consumer']}
        if 'worker' in include:
            worker_graph = worker.blueprint.graph
            if 'consumer' in include:
                worker.blueprint.connect_with(worker.consumer.blueprint)
        else:
            worker_graph = worker.consumer.blueprint.graph
        worker_graph.to_dot(self.stdout)

    def workers(self, *args, **kwargs):

        def simplearg(arg):
            return maybe_list(itemgetter(0, 2)(arg.partition(':')))

        def maybe_list(l, sep=','):
            return (l[0], l[1].split(sep) if sep in l[1] else l[1])

        args = dict(simplearg(arg) for arg in args)
        generic = 'generic' in args

        def generic_label(node):
            return '{0} ({1}://)'.format(type(node).__name__,
                                         node._label.split('://')[0])

        @python_2_unicode_compatible
        class Node(object):
            force_label = None
            scheme = {}

            def __init__(self, label, pos=None):
                self._label = label
                self.pos = pos

            def label(self):
                return self._label

            def __str__(self):
                return self.label()

        class Thread(Node):
            scheme = {
                'fillcolor': 'lightcyan4',
                'fontcolor': 'yellow',
                'shape': 'oval',
                'fontsize': 10,
                'width': 0.3,
                'color': 'black',
            }

            def __init__(self, label, **kwargs):
                self.real_label = label
                super(Thread, self).__init__(
                    label='thr-{0}'.format(next(tids)),
                    pos=0,
                )

        class Formatter(GraphFormatter):

            def label(self, obj):
                return obj and obj.label()

            def node(self, obj):
                scheme = dict(obj.scheme) if obj.pos else obj.scheme
                if isinstance(obj, Thread):
                    scheme['label'] = obj.real_label
                return self.draw_node(
                    obj, dict(self.node_scheme, **scheme),
                )

            def terminal_node(self, obj):
                return self.draw_node(
                    obj, dict(self.term_scheme, **obj.scheme),
                )

            def edge(self, a, b, **attrs):
                if isinstance(a, Thread):
                    attrs.update(arrowhead='none', arrowtail='tee')
                return self.draw_edge(a, b, self.edge_scheme, attrs)

        def subscript(n):
            S = {'0': '₀', '1': '₁', '2': '₂', '3': '₃', '4': '₄',
                 '5': '₅', '6': '₆', '7': '₇', '8': '₈', '9': '₉'}
            return ''.join([S[i] for i in str(n)])

        class Worker(Node):
            pass

        class Backend(Node):
            scheme = {
                'shape': 'folder',
                'width': 2,
                'height': 1,
                'color': 'black',
                'fillcolor': 'peachpuff3',
            }

            def label(self):
                return generic_label(self) if generic else self._label

        class Broker(Node):
            scheme = {
                'shape': 'circle',
                'fillcolor': 'cadetblue3',
                'color': 'cadetblue4',
                'height': 1,
            }

            def label(self):
                return generic_label(self) if generic else self._label

        from itertools import count
        tids = count(1)
        Wmax = int(args.get('wmax', 4) or 0)
        Tmax = int(args.get('tmax', 3) or 0)

        def maybe_abbr(l, name, max=Wmax):
            size = len(l)
            abbr = max and size > max
            if 'enumerate' in args:
                l = ['{0}{1}'.format(name, subscript(i + 1))
                     for i, obj in enumerate(l)]
            if abbr:
                l = l[0:max - 1] + [l[size - 1]]
                l[max - 2] = '{0}⎨…{1}⎬'.format(
                    name[0], subscript(size - (max - 1)))
            return l

        try:
            workers = args['nodes']
            threads = args.get('threads') or []
        except KeyError:
            replies = self.app.control.inspect().stats() or {}
            workers, threads = [], []
            for worker, reply in items(replies):
                workers.append(worker)
                threads.append(reply['pool']['max-concurrency'])

        wlen = len(workers)
        backend = args.get('backend', self.app.conf.result_backend)
        threads_for = {}
        workers = maybe_abbr(workers, 'Worker')
        if Wmax and wlen > Wmax:
            threads = threads[0:3] + [threads[-1]]
        for i, threads in enumerate(threads):
            threads_for[workers[i]] = maybe_abbr(
                list(range(int(threads))), 'P', Tmax,
            )

        broker = Broker(args.get(
            'broker', self.app.connection_for_read().as_uri()))
        backend = Backend(backend) if backend else None
        deps = DependencyGraph(formatter=Formatter())
        deps.add_arc(broker)
        if backend:
            deps.add_arc(backend)
        curworker = [0]
        for i, worker in enumerate(workers):
            worker = Worker(worker, pos=i)
            deps.add_arc(worker)
            deps.add_edge(worker, broker)
            if backend:
                deps.add_edge(worker, backend)
            threads = threads_for.get(worker._label)
            if threads:
                for thread in threads:
                    thread = Thread(thread)
                    deps.add_arc(thread)
                    deps.add_edge(thread, worker)

            curworker[0] += 1

        deps.to_dot(self.stdout)
