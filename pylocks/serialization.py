from __future__ import print_function
import pickle
from pylocks.errors import SerializationError

def serialize_impl(x):
    return pickle.dumps(x, 2)

def deserialize_impl(x):
    return pickle.loads(x)


class Serializable(object):
    def serialize(self):
        return serialize_impl(self)

    @classmethod
    def deserialize(cls, data):
        instance = deserialize_impl(data)
        if not isinstance(instance, cls):
            raise SerializationError(data)
        return instance
