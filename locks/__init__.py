

class KeyFormatter(object):
    class ArityError(ValueError):
        pass
    def __init__(self, prefix, arity=1):
        self.prefix = prefix
        self.arity = 1

    def _make_key(self, args):
        key_part = ':'.join(list(map(str, list(args))))
        return '%s:%s' % (self.prefix, key_part)

    def _check_arity(self, args):
        if len(args) != self.arity:
            raise ArityError("expected %i args; got %i" % (self.arity, len(args)))

    def format(self, *args):
        self._check_arity(args)
        return self._make_key(args)
