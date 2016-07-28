import time
from pylocks.util import make_id
from .key_formatter import KeyFormatter
from .lock_request import LockRequest

class LockSettings(object):
    def __init__(self, prefix, ttl, arity, root_prefix='pylocks'):
        self.prefix = prefix
        self.ttl = ttl
        self.arity = arity
        self.root_prefix = root_prefix

    @property
    def _formatter(self):
        return KeyFormatter(prefix=self.prefix, arity=self.arity, root_prefix=self.root_prefix)

    def make_request(self, args_list, now=None):
        key = self._formatter.format(args_list)
        now = now or time.time()
        return LockRequest(
            key=key,
            request_time=now,
            initial_ttl=self.ttl,
            lock_arity=self.arity,
            lock_prefix=self.prefix,
            root_prefix=self.root_prefix
        )


