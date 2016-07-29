

class LockRequest(object):
    def __init__(self, key, request_time, initial_ttl, lock_arity, lock_prefix, root_prefix='pylocks'):
        self.key = key
        self.request_time = request_time
        self.initial_ttl = initial_ttl
        self.lock_arity = lock_arity
        self.lock_prefix = lock_prefix
        self.root_prefix = root_prefix

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.key == other.key
        return self.key == other

