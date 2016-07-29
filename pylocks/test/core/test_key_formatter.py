import unittest
from pylocks.core.key_formatter import KeyFormatter
from pylocks.errors import ArityError

class TestKeyFormatter(unittest.TestCase):
    def test_format_arity(self):
        fmt = KeyFormatter(prefix='foo', arity=1, root_prefix='root')
        with self.assertRaises(ArityError):
            fmt.format('x', 'y')
        with self.assertRaises(ArityError):
            fmt.format()
        self.assertEqual('root:foo:x', fmt.format('x'))
