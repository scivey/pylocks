import unittest
import json
import time
from redis import WatchError

from pylocks.errors import LockAlreadyHeld, LockExpired, LockNotOwned, InvalidLockValue
from pylocks.util import make_id
from pylocks.core.key_formatter import KeyFormatter
from pylocks.core.lock_settings import LockSettings
from .single_lock_handle import SingleLockHandle
from .base_redis_lock import BaseRedisLock


class RedisLock(object):
    def __init__(self, settings, redis_conn):
        self.settings = settings
        self.redis_conn = redis_conn
        self.base_lock = BaseRedisLock(redis_conn=redis_conn)

    @property
    def arity(self):
        return self.settings.arity

    @property
    def ttl(self):
        return self.settings.ttl

    @property
    def prefix(self):
        return self.settings.prefix

    def make_key(self, args_list):
        return self.settings.make_request(args_list).key

    def is_held(self, args_list):
        """
        returns True if anyone owns the lock for
        the given args list.
        """
        request = self.settings.make_request(args_list)
        return self.base_lock.is_held(self.make_key(args_list))

    def acquire(self, args_list):
        """
        acquires a lock with key corresponding to `args_list`.
        returns a `LockHandle` on success with the ID and time of
        acquisition.
        raises an `LockAlreadyHeld` exception on failure.
        """
        request = self.settings.make_request(args_list)
        return self.base_lock.acquire(self.settings.make_request(args_list))

    def macquire(self, args_lists):
        """
        Attempt to acquire multiple locks simultaneously.

        `args_lists` should be a list of tuples/lists when lock arity >= 2.
            E.g. :
                lock.macquire([('x', '1'), ('y', '1')])
        for locks of arity == 1, either of these forms will work:
                lock.macquire(['x', 'y'])
                lock.macquire([('x',), ('y',)])

        returns a tuple of:
            - a dict mapping arg_lists to successful handles
            - a list containins the args_list members which could not be locked

        """
        now = time.time()
        requests = []
        req_to_args = {}
        for one_args_list in args_lists:
            req = self.settings.make_request(args_list=one_args_list, now=now)
            requests.append(req)
            req_to_args[req] = one_args_list

        locked, missing = self.base_lock.macquire(requests)
        locked_by_args = {}
        for req, handle in locked.items():
            locked_by_args[req_to_args[req]] = handle
        missing_by_args = [req_to_args[req] for req in missing]
        return locked_by_args, missing_by_args

    def mrelease_expected(self, args_lists_to_ids):
        released = []
        missing = []
        for arg_list, expected in args_lists_to_ids.items():
            req = self.settings.make_request(arg_list)
            try:
                self.base_lock.release_expected(req.key, expected)
                released.append(args_list)
            except LockNotOwned:
                missing.append(arg_list)
        return released, missing

    def release_expected(self, args_list, expected_id):
        """
        Release the key corresponding to `args_list`,
        *if* its current ID matches `expected_id`.
        """
        req = self.settings.make_request(args_list)
        return self.base_lock.release_expected(req.key, expected_id)

    def release_hard(self, args_list):
        """
        Releases any locks held on the key corresponding to `args_list`,
        without checking acquisition IDs.
        """
        key = self.settings.make_request(args_list).key
        result = self.redis_conn.delete(key)
        if not result:
            raise LockNotOwned(key)

    @classmethod
    def create(cls, prefix, ttl, redis_conn, arity=1):
        settings = LockSettings(prefix=prefix, ttl=ttl, arity=arity)
        return cls(settings=settings, redis_conn=redis_conn)



class TestRedisLock(unittest.TestCase):
    def setUp(self):
        import redislite
        self.r = redislite.StrictRedis()

    def make_lock(self):
        return RedisLock.create(prefix='foo', ttl=60, redis_conn=self.r, arity=1)

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
        with self.assertRaises(LockAlreadyHeld):
            lock.acquire('x')
        with self.assertRaises(LockAlreadyHeld):
            self.make_lock().acquire('x')

    def test_not_locked(self):
        lock = RedisLock.create(prefix='foo', ttl=60, redis_conn=self.r, arity=1)
        result = lock.acquire('x')
        self.assertTrue(isinstance(result, SingleLockHandle))

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
        self.assertTrue(isinstance(handles['x'], SingleLockHandle))

    def test_macquire_2(self):
        lock = self.make_lock()
        existing = lock.acquire('y')
        handles, missing = lock.macquire(['x', 'y', 'z'])
        self.assertEqual(['y'], missing)
        self.assertEqual({'x' , 'z'}, set(list(handles.keys())))
        self.assertTrue(isinstance(handles['x'], SingleLockHandle))

    def test_releasing_1(self):
        lock = self.make_lock()
        self.assertFalse(lock.is_held('x'))
        handle = lock.acquire('x')
        self.assertTrue(lock.is_held('x'))

