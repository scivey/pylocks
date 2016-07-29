import time
from pylocks.errors import LockAlreadyHeld, LockNotOwned
from pylocks.util import make_id
from pylocks.core.lock_lease_data import LockLeaseData
from .blocking_redis_lease_handle import BlockingRedisLeaseHandle

class BaseBlockingRedisLock(object):
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
        attempt to satisfy a `pylocks.core.lock_request.LockRequest`.

        if successful, returns a `BlockingRedisLeaseHandle` containing
        the unique lease id and time of acquisition

        raises a `LockAlreadyHeld` exception on failure.
        """
        new_id = make_id()
        now = time.time()
        possible_handle = BlockingRedisLeaseHandle(
            redis_conn=self.redis_conn,
            handle_data=LockLeaseData(
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

        accepts a list of `pylocks.core.lock_request.LockRequest` objects.

        returns a tuple of:
            - a dict mapping lock requests to successfully obtained leases
            - a list containing the requests which could not be met

        """
        now = time.time()
        to_set = []
        for request in lock_requests:
            possible_handle = BlockingRedisLeaseHandle(
                redis_conn=self.redis_conn,
                handle_data=LockLeaseData(
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
        Release the lock on `key` *if* the current lease ID
        matches `expected_id`.
        """
        possible_handle = self.LockHandle.get_lease_handle(
            key=key, id=expected_id, redis_conn=self.redis_conn
        )
        possible_handle.release()

    def release_hard(self, key):
        """
        Releases any locks held on `key`,
        without checking lease IDs.
        """
        result = self.redis_conn.delete(key)
        if not result:
            raise LockNotOwned(key)

    def get_lease_handle(self, key, expected_id):
        return BlockingRedisLeaseHandle.get_existing(
            key=key,
            expected_id=expected_id,
            redis_conn=self.redis_conn
        )

