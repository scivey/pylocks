import unittest
from pylocks.blocking.blocking_redis_lock import BlockingRedisLockFactory
from pylocks.blocking.blocking_redis_lease_handle import BlockingRedisLeaseHandle
from pylocks.errors import LockAlreadyHeld, LockNotOwned


class TestBlockingRedisLock(unittest.TestCase):
    def setUp(self):
        import redislite
        self.r = redislite.StrictRedis()
        runner_self = self

        class TestFactory(BlockingRedisLockFactory):
            def get_redis_connection(self):
                return runner_self.r

        self.lock_factory = TestFactory(
            prefix='foo', ttl=60, arity=1
        )

    def make_lock(self):
        return self.lock_factory.build()

    def test_already_locked(self):
        lock = self.make_lock()
        handle = lock.acquire('x')
        with self.assertRaises(LockAlreadyHeld):
            lock.acquire('x')
        with self.assertRaises(LockAlreadyHeld):
            self.make_lock().acquire('x')
        handle.release()
        lock = self.make_lock()
        handle_2 = lock.acquire('x')
        handle_2.check_if_owned()
        with self.assertRaises(LockAlreadyHeld):
            lock.acquire('x')
        with self.assertRaises(LockAlreadyHeld):
            self.make_lock().acquire('x')

    def test_not_locked(self):
        lock = self.make_lock()
        result = lock.acquire('x')
        self.assertTrue(isinstance(result, BlockingRedisLeaseHandle))

    def test_handle_still_holding_1(self):
        lock = self.make_lock()
        handle = lock.acquire('x')
        self.assertTrue(handle.do_i_still_have_lock())
        self.r.delete(lock.make_key('x'))
        self.assertFalse(handle.do_i_still_have_lock())

    def test_handle_still_holding_2(self):
        lock = self.make_lock()
        handle = lock.acquire('x')
        self.assertTrue(handle.do_i_still_have_lock())
        self.r.delete(lock.make_key('x'))
        handle_2 = lock.acquire('x')
        self.assertTrue(handle_2.do_i_still_have_lock())
        self.assertFalse(handle.do_i_still_have_lock())

    def test_handle_still_holding_3(self):
        lock = self.make_lock()
        handle = lock.acquire('x')
        self.assertTrue(handle.do_i_still_have_lock())
        self.r.delete(lock.make_key('x'))
        with self.assertRaises(LockNotOwned):
            handle.check_if_owned()

    def test_macquire_1(self):
        lock = self.make_lock()
        handles, missing = lock.macquire(['x', 'y', 'z'])
        self.assertEqual([], missing)
        self.assertEqual({'x' ,'y', 'z'}, set(list(handles.keys())))
        self.assertTrue(isinstance(handles['x'], BlockingRedisLeaseHandle))

    def test_macquire_2(self):
        lock = self.make_lock()
        lock.acquire('y')
        handles, missing = lock.macquire(['x', 'y', 'z'])
        self.assertEqual(['y'], missing)
        self.assertEqual({'x' , 'z'}, set(list(handles.keys())))
        self.assertTrue(isinstance(handles['x'], BlockingRedisLeaseHandle))

    def test_releasing_1(self):
        lock = self.make_lock()
        self.assertFalse(lock.is_held('x'))
        lock.acquire('x')
        self.assertTrue(lock.is_held('x'))

