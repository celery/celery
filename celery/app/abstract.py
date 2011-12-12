from __future__ import absolute_import


class from_config(object):

    def __init__(self, key=None):
        self.key = key

    def get_key(self, attr):
        return attr if self.key is None else self.key


class _configurated(type):

    def __new__(cls, name, bases, attrs):
        attrs["__confopts__"] = dict((attr, spec.get_key(attr))
                                          for attr, spec in attrs.iteritems()
                                              if isinstance(spec, from_config))
        inherit_from = attrs.get("inherit_confopts", ())
        for subcls in bases:
            try:
                attrs["__confopts__"].update(subcls.__confopts__)
            except AttributeError:
                pass
        for subcls in inherit_from:
            attrs["__confopts__"].update(subcls.__confopts__)
        attrs = dict((k, v if not isinstance(v, from_config) else None)
                        for k, v in attrs.iteritems())
        return super(_configurated, cls).__new__(cls, name, bases, attrs)


class configurated(object):
    __metaclass__ = _configurated

    def setup_defaults(self, kwargs, namespace="celery"):
        confopts = self.__confopts__
        app, find = self.app, self.app.conf.find_value_for_key

        for attr, keyname in confopts.iteritems():
            try:
                value = kwargs[attr]
            except KeyError:
                value = find(keyname, namespace)
            else:
                if value is None:
                    value = find(keyname, namespace)
            setattr(self, attr, value)

        for attr_name, attr_value in kwargs.iteritems():
            if attr_name not in confopts and attr_value is not None:
                setattr(self, attr_name, attr_value)

    def confopts_as_dict(self):
        return dict((key, getattr(self, key))
                        for key in self.__confopts__.iterkeys())
