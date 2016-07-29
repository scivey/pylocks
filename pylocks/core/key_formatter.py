from pylocks.errors import ArityError
from pylocks.conf import DEFAULT_ROOT_PREFIX

class KeyFormatter(object):
    def __init__(self, prefix, arity=1, root_prefix=DEFAULT_ROOT_PREFIX):
        self.prefix = prefix
        self.arity = 1
        self.root_prefix = root_prefix

    def _make_key(self, args):
        if not isinstance(args, (list, tuple)) and self.arity == 1:
            args = [args]
        key_part = ':'.join(list(map(str, list(args))))
        return '%s:%s:%s' % (self.root_prefix, self.prefix, key_part)

    def _check_arity(self, args):
        if len(args) != self.arity:
            raise ArityError("expected %i args; got %i" % (self.arity, len(args)))

    def format(self, *args):
        self._check_arity(args)
        return self._make_key(args)

