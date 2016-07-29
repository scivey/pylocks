import unittest
from pylocks.core.lock_settings import LockSettings
from pylocks.core.lock_request import LockRequest

class TestLockSettings(unittest.TestCase):
    def test_make_request(self):
        def make_kwargs():
            return dict(
                prefix='some-key',
                ttl=307,
                arity=2,
                root_prefix='test_lock_settings'
            )

        settings = LockSettings(**make_kwargs())
        request = settings.make_request(['x', 'y'], now=500)
        self.assertTrue(isinstance(request, LockRequest))
        kwargs = make_kwargs()
        self.assertEqual(kwargs['prefix'], request.lock_prefix)
        self.assertEqual(kwargs['arity'], request.lock_arity)
        self.assertEqual(kwargs['ttl'], request.initial_ttl)
        self.assertEqual(kwargs['root_prefix'], request.root_prefix)
        for part in ('some-key', 'x', 'y'):
            self.assertTrue(part in request.key)
