import time

from pylocks.testing.redis_test import RedisTest
from pylocks.core.lock_handle_data import LockHandleData
from pylocks.core.lock_request import LockRequest
from pylocks.errors import LockNotHeld
from .single_lock_handle import SingleLockHandle
from .base_redis_lock import BaseRedisLock

def make_handle_data(key, handle_id, ttl=20):
    now = time.time()
    request_time = now - (ttl / 2)
    acquire_time = request_time + 1.5
    data = LockHandleData(
        request=LockRequest(
            key=key,
            initial_ttl=ttl,
            request_time=request_time,
            lock_arity=1,
            lock_prefix='foo',
            root_prefix='testlocks'
        ),
        id=handle_id,
        acquired_at=acquire_time
    )
    return data

class TestSingleLockHandle(RedisTest):
    def setUp(self):
        super(TestSingleLockHandle, self).setUp()
        self.lock = BaseRedisLock(redis_conn=self.r)

    def make_handle(self, key, handle_id):
        data = make_handle_data(key, handle_id)
        return SingleLockHandle(handle_data=data, redis_conn=self.r)

    def test_check_if_owned_1(self):
        handle = self.make_handle('some-lock', 'some-handle-id')
        with self.assertRaises(LockNotHeld):
            handle.check_if_owned()

    def test_check_if_owned_2(self):
        handle = self.make_handle('some-lock', 'some-handle-id')
        self.lock._debug_hard_set_handle(handle)
        handle.check_if_owned()

