import time
import unittest
from pylocks.errors import LockAlreadyHeld, LockNotOwned
from pylocks.blocking.blocking_redis_lease_handle import BlockingRedisLeaseHandle
from pylocks.blocking.base_blocking_redis_lock import BaseBlockingRedisLock
from pylocks.core.lock_request import LockRequest

class TestBaseBlockingRedisLock(unittest.TestCase):
    def setUp(self):
        import redislite
        self.r = redislite.StrictRedis()

    def make_lock(self):
        return BaseBlockingRedisLock(redis_conn=self.r)

    def make_request(self, key, ttl=20):
        return LockRequest(
            key=key,
            request_time=time.time(),
            initial_ttl=ttl,
            lock_arity=1,
            lock_prefix='lock',
            root_prefix='pylocks:test'
        )

    def test_already_locked(self):
        lock = self.make_lock()
        handle = lock.acquire(self.make_request('x'))

        with self.assertRaises(LockAlreadyHeld):
            lock.acquire(self.make_request('x'))
        with self.assertRaises(LockAlreadyHeld):
            self.make_lock().acquire(self.make_request('x'))
        handle.release()
        lock = self.make_lock()
        lock.acquire(self.make_request('x'))
        with self.assertRaises(LockAlreadyHeld):
            lock.acquire(self.make_request('x'))
        with self.assertRaises(LockAlreadyHeld):
            self.make_lock().acquire(self.make_request('x'))

    def test_not_locked(self):
        lock = self.make_lock()
        result = lock.acquire(self.make_request('x'))
        self.assertTrue(isinstance(result, BlockingRedisLeaseHandle))

    def test_handle_still_holding_1(self):
        lock = self.make_lock()
        handle = lock.acquire(self.make_request('x'))
        self.assertTrue(handle.do_i_still_have_lock())
        self.r.delete(self.make_request('x').key)
        self.assertFalse(handle.do_i_still_have_lock())

    def test_handle_still_holding_2(self):
        lock = self.make_lock()
        handle = lock.acquire(self.make_request('x'))
        self.assertTrue(handle.do_i_still_have_lock())
        self.r.delete(self.make_request('x').key)
        handle_2 = lock.acquire(self.make_request('x'))
        self.assertTrue(handle_2.do_i_still_have_lock())
        self.assertFalse(handle.do_i_still_have_lock())

    def test_handle_still_holding_3(self):
        lock = self.make_lock()
        handle = lock.acquire(self.make_request('x'))
        self.assertTrue(handle.do_i_still_have_lock())
        self.r.delete(self.make_request('x').key)
        with self.assertRaises(LockNotOwned):
            handle.check_if_owned()

    def test_macquire_1(self):
        lock = self.make_lock()
        reqs = list(map(self.make_request, ['x', 'y', 'z']))
        handles, missing = lock.macquire(reqs)
        self.assertEqual([], missing)
        self.assertEqual({'x' ,'y', 'z'}, set([h.key for h in handles.keys()]))
        self.assertTrue(isinstance(handles['x'], BlockingRedisLeaseHandle))

    def test_macquire_2(self):
        lock = self.make_lock()
        lock.acquire(self.make_request('y'))
        reqs = list(map(self.make_request, ['x', 'y', 'z']))
        handles, missing = lock.macquire(reqs)
        self.assertEqual(['y'], [m.key for m in missing])
        self.assertEqual({'x' , 'z'}, set([h.key for h in handles.keys()]))
        req = self.make_request('x')
        self.assertTrue(isinstance(handles[req], BlockingRedisLeaseHandle))

    def test_releasing_1(self):
        lock = self.make_lock()
        self.assertFalse(lock.is_held('x'))
        lock.acquire(self.make_request('x'))
        self.assertTrue(lock.is_held('x'))

