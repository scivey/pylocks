import pickle
from pylocks.errors import SerializationError

def serialize(x):
    return pickle.dumps(x, 2)

def deserialize(x):
    return pickle.loads(x)


class Serializable(object):
    def serialize(self):
        return serialize(self)

    @classmethod
    def deserialize(cls, data):
        instance = deserialize(data)
        if not isinstance(instance, cls):
            raise SerializationError(data)
        return instance
