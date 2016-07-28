import unittest
import json
import time
from pylocks.errors import LockAlreadyHeld, LockExpired, LockNotOwned, InvalidLockValue
from pylocks.core.key_formatter import KeyFormatter
from pylocks.util import make_id
from pylocks.core.lock_handle_data import LockHandleData
from pylocks.core.lock_request import LockRequest
from .single_lock_handle import SingleLockHandle
from redis import WatchError

class BaseRedisLock(object):
    def __init__(self, redis_conn):
        self.redis_conn = redis_conn

    def is_held(self, key):
        """
        returns True if anyone owns the lock for
        the given key
        """
        return self.redis_conn.get(key) is not None

    def _debug_hard_set_handle(self, lock_handle):
        lock_request = lock_handle.handle_data.request
        temp_key = '%s-%s' % (lock_request.key, lock_handle.id)
        pipe = self.redis_conn.pipeline()
        pipe.setex(
            name=temp_key,
            time=lock_request.initial_ttl,
            value=lock_handle.serialize()
        )
        pipe.rename(temp_key, lock_request.key)
        result = pipe.execute()
        got_lock = result[1]
        assert got_lock
        return lock_handle

    def acquire(self, lock_request):
        """
        acquires a lock on `key`
        returns a `LockHandle` on success with the ID and time of
        acquisition.
        raises an `LockAlreadyHeld` exception on failure.
        """
        new_id = make_id()
        now = time.time()
        possible_handle = SingleLockHandle(
            redis_conn=self.redis_conn,
            handle_data=LockHandleData(
                request=lock_request,
                id=new_id,
                acquired_at=now
            )
        )
        temp_key = '%s-%s' % (lock_request.key, new_id)
        pipe = self.redis_conn.pipeline()
        pipe.setex(
            name=temp_key,
            time=lock_request.initial_ttl,
            value=possible_handle.serialize()
        )
        pipe.renamenx(temp_key, lock_request.key)
        result = pipe.execute()
        got_lock = result[1]
        if got_lock:
            return possible_handle
        raise LockAlreadyHeld(lock_request.key)

    def macquire(self, lock_requests):
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
        to_set = []
        for request in lock_requests:
            possible_handle = SingleLockHandle(
                redis_conn=self.redis_conn,
                handle_data=LockHandleData(
                    id=make_id(),
                    acquired_at=now,
                    request=request
                )
            )
            to_set.append((request, possible_handle))

        pipe = self.redis_conn.pipeline()
        for request, handle in to_set:
            temp_key = '%s-%s' % (handle.key, handle.id)
            pipe.setex(name=temp_key, time=request.initial_ttl, value=handle.serialize())
            pipe.renamenx(temp_key, handle.key)
        results = list(pipe.execute())
        acquired = {}
        missing = []
        for i, (arg_list, handle) in enumerate(to_set):
            result_offset = 2 * i
            obtained = results[result_offset+1]
            if obtained:
                acquired[arg_list] = handle
            else:
                missing.append(arg_list)
        return acquired, missing

    def mrelease_expected(self, keys_to_ids):
        released = []
        missing = []
        for key, expected_id in keys_to_ids.items():
            try:
                self.release_expected(key, expected_id)
                released.append(key)
            except LockNotOwned:
                missing.append(key)
        return released, missing

    def release_expected(self, key, expected_id):
        """
        Release the key corresponding to `args_list`,
        *if* its current ID matches `expected_id`.
        """
        possible_handle = self.LockHandle.get_handle(
            key=key, id=expected_id, redis_conn=self.redis_conn
        )
        possible_handle.release()

    def release_hard(self, key):
        """
        Releases any locks held on the key corresponding to `args_list`,
        without checking acquisition IDs.
        """
        result = self.redis_conn.delete(key)
        if not result:
            raise LockNotOwned(key)

    def get_handle(self, key, expected_id):
        return SingleLockHandle.get_handle(
            key=key,
            expected_id=expected_id,
            redis_conn=self.redis_conn
        )


class TestBaseRedisLock(unittest.TestCase):
    def setUp(self):
        import redislite
        self.r = redislite.StrictRedis()

    def make_lock(self):
        return BaseRedisLock(redis_conn=self.r)

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
        handle_2 = lock.acquire(self.make_request('x'))
        with self.assertRaises(LockAlreadyHeld):
            lock.acquire(self.make_request('x'))
        with self.assertRaises(LockAlreadyHeld):
            self.make_lock().acquire(self.make_request('x'))

    def test_not_locked(self):
        lock = self.make_lock()
        result = lock.acquire(self.make_request('x'))
        self.assertTrue(isinstance(result, SingleLockHandle))

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
        self.assertTrue(isinstance(handles['x'], SingleLockHandle))

    def test_macquire_2(self):
        lock = self.make_lock()
        existing = lock.acquire(self.make_request('y'))
        reqs = list(map(self.make_request, ['x', 'y', 'z']))
        handles, missing = lock.macquire(reqs)
        self.assertEqual(['y'], [m.key for m in missing])
        self.assertEqual({'x' , 'z'}, set([h.key for h in handles.keys()]))
        req = self.make_request('x')
        self.assertTrue(isinstance(handles[req], SingleLockHandle))

    def test_releasing_1(self):
        lock = self.make_lock()
        self.assertFalse(lock.is_held('x'))
        handle = lock.acquire(self.make_request('x'))
        self.assertTrue(lock.is_held('x'))

