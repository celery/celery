"""

Custom Django Model Fields.

"""
from django.db import models
from django.conf import settings

try:
    import cPickle as pickle
except ImportError:
    import pickle


class PickledObject(str):
    """A subclass of string so it can be told whether a string is
       a pickled object or not (if the object is an instance of this class
       then it must [well, should] be a pickled one)."""
    pass


if settings.DATABASE_ENGINE == "postgresql_psycopg2":
    import psycopg2.extensions
    # register PickledObject as a QuotedString otherwise we will see
    # can't adapt errors from psycopg2.
    psycopg2.extensions.register_adapter(PickledObject, psycopg2.extensions.QuotedString)


class PickledObjectField(models.Field):
    """A field that automatically pickles/unpickles its value."""
    __metaclass__ = models.SubfieldBase

    def to_python(self, value):
        """Convert the database value to a python value."""
        if isinstance(value, PickledObject):
            # If the value is a definite pickle; and an error is
            # raised in de-pickling it should be allowed to propogate.
            return pickle.loads(str(value))
        else:
            try:
                return pickle.loads(str(value))
            except Exception:
                # If an error was raised, just return the plain value
                return value

    def get_db_prep_save(self, value):
        """get_db_prep_save"""
        if value is not None and not isinstance(value, PickledObject):
            value = PickledObject(pickle.dumps(value))
        return value

    def get_internal_type(self):
        """The database field type used by this field."""
        return 'TextField'

    def get_db_prep_lookup(self, lookup_type, value):
        """get_db_prep_lookup"""
        if lookup_type == 'exact':
            value = self.get_db_prep_save(value)
            return super(PickledObjectField, self).get_db_prep_lookup(
                    lookup_type, value)
        elif lookup_type == 'in':
            value = [self.get_db_prep_save(v) for v in value]
            return super(PickledObjectField, self).get_db_prep_lookup(
                    lookup_type, value)
        else:
            raise TypeError('Lookup type %s is not supported.' % lookup_type)
