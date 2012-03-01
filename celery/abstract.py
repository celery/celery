# -*- coding: utf-8 -*-
"""
    celery.abstract
    ~~~~~~~~~~~~~~~

    Implements components and boot-steps.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from collections import defaultdict
from importlib import import_module

from .datastructures import DependencyGraph
from .utils import instantiate


class Namespace(object):
    """A namespace containing components.

    Every component must belong to a namespace.

    When component classes are created they are added to the
    mapping of unclaimed components.  The components will be
    claimed when the namespace they belong to is created.

    :keyword name: Set the name of this namespace.
    :keyword app: Set the Celery app for this namespace.

    """
    name = None
    _unclaimed = defaultdict(dict)
    _started_count = 0

    def __init__(self, name=None, app=None, logger=None):
        self.app = app
        self.name = name or self.name
        self.logger = logger or self.app.log.get_default_logger()
        self.services = []

    def modules(self):
        """Subclasses can override this to return a
        list of modules to import before components are claimed."""
        return []

    def load_modules(self):
        """Will load the component modules this namespace depends on."""
        for m in self.modules():
            self.import_module(m)

    def apply(self, parent, **kwargs):
        """Apply the components in this namespace to an object.

        This will apply the ``__init__`` and ``include`` methods
        of each components with the object as argument.

        For ``StartStopComponents`` the services created
        will also be added the the objects ``components`` attribute.

        """
        self._debug("Loading modules.")
        self.load_modules()
        self._debug("Claiming components.")
        self.components = self._claim()
        self._debug("Building boot step graph.")
        self.boot_steps = [self.bind_component(name, parent, **kwargs)
                                for name in self._finalize_boot_steps()]
        self._debug("New boot order: %r", [c.name for c in self.boot_steps])

        for component in self.boot_steps:
            component.include(parent)
        return self

    def bind_component(self, name, parent, **kwargs):
        """Bind component to parent object and this namespace."""
        comp = self[name](parent, **kwargs)
        comp.namespace = self
        return comp

    def import_module(self, module):
        return import_module(module)

    def __getitem__(self, name):
        return self.components[name]

    def _find_last(self):
        for C in self.components.itervalues():
            if C.last:
                return C

    def _finalize_boot_steps(self):
        G = self.graph = DependencyGraph((C.name, C.requires)
                            for C in self.components.itervalues())
        last = self._find_last()
        if last:
            for obj in G:
                if obj != last.name:
                    G.add_edge(last.name, obj)
        return G.topsort()

    def _claim(self):
        return self._unclaimed[self.name]

    def _debug(self, msg, *args):
        return self.logger.debug("[%s] " + msg,
                                *(self.name.capitalize(), ) + args)


class ComponentType(type):
    """Metaclass for components."""

    def __new__(cls, name, bases, attrs):
        abstract = attrs.pop("abstract", False)
        if not abstract:
            try:
                cname = attrs["name"]
            except KeyError:
                raise NotImplementedError("Components must be named")
            namespace = attrs.get("namespace", None)
            if not namespace:
                attrs["namespace"], _, attrs["name"] = cname.partition('.')
        cls = super(ComponentType, cls).__new__(cls, name, bases, attrs)
        if not abstract:
            Namespace._unclaimed[cls.namespace][cls.name] = cls
        return cls


class Component(object):
    """A component.

    The :meth:`__init__` method is called when the component
    is bound to a parent object, and can as such be used
    to initialize attributes in the parent object at
    parent instantiation-time.

    """
    __metaclass__ = ComponentType

    #: The name of the component, or the namespace
    #: and the name of the component separated by dot.
    name = None

    #: List of component names this component depends on.
    #: Note that the dependencies must be in the same namespace.
    requires = ()

    #: can be used to specify the namespace,
    #: if the name does not include it.
    namespace = None

    #: if set the component will not be registered,
    #: but can be used as a component base class.
    abstract = True

    #: Optional obj created by the :meth:`create` method.
    #: This is used by StartStopComponents to keep the
    #: original service object.
    obj = None

    #: This flag is reserved for the workers Consumer,
    #: since it is required to always be started last.
    #: There can only be one object marked with lsat
    #: in every namespace.
    last = False

    #: This provides the default for :meth:`include_if`.
    enabled = True

    def __init__(self, parent, **kwargs):
        pass

    def create(self, parent):
        """Create the component."""
        pass

    def include_if(self, parent):
        """An optional predicate that decided whether this
        component should be created."""
        return self.enabled

    def instantiate(self, qualname, *args, **kwargs):
        return instantiate(qualname, *args, **kwargs)

    def include(self, parent):
        if self.include_if(parent):
            self.obj = self.create(parent)
            return True


class StartStopComponent(Component):
    abstract = True
    terminable = False

    def start(self):
        return self.obj.start()

    def stop(self):
        return self.obj.stop()

    def terminate(self):
        if self.terminable:
            return self.obj.terminate()
        return self.obj.stop()

    def include(self, parent):
        if super(StartStopComponent, self).include(parent):
            parent.components.append(self.obj)
