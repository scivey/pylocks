import unittest
from .errors import ArityError

class KeyFormatter(object):
    def __init__(self, prefix, arity=1, root_prefix='pylocks'):
        self.prefix = prefix
        self.arity = 1
        self.root_prefix = root_prefix

    def _make_key(self, args):
        if not isinstance(args, (list, tuple)) and self.arity == 1:
            args_list = [args_list]
        key_part = ':'.join(list(map(str, list(args))))
        return '%s:%s:%s' % (self.root_prefix, self.prefix, key_part)

    def _check_arity(self, args):
        if len(args) != self.arity:
            raise ArityError("expected %i args; got %i" % (self.arity, len(args)))

    def format(self, *args):
        self._check_arity(args)
        return self._make_key(args)


class TestKeyFormatter(unittest.TestCase):
    def test_format_arity(self):
        fmt = KeyFormatter(prefix='foo', arity=1, root_prefix='root')
        with self.assertRaises(ArityError):
            fmt.format('x', 'y')
        with self.assertRaises(ArityError):
            fmt.format()
        self.assertEqual('root:foo:x', fmt.format('x'))
