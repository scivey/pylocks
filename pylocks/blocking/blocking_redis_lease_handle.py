from __future__ import print_function
from pylocks.core.lock_lease_data import LockLeaseData
from pylocks.errors import LockExpired, LockNotOwned
from redis import WatchError
import contextlib

class BlockingRedisLeaseHandle(object):
    def __init__(self, handle_data, redis_conn):
        self.handle_data = handle_data
        self.redis_conn = redis_conn

    @property
    def key(self):
        return self.handle_data.key

    @property
    def id(self):
        return self.handle_data.id

    def serialize(self):
        return self.handle_data.serialize()

    @classmethod
    def deserialize(cls, data, redis_conn):
        return cls(handle_data=LockLeaseData.deserialize(data), redis_conn=redis_conn)

    def _check_if_same_id(self, raw_response):
        result = LockLeaseData.deserialize(raw_response)
        return result.id == self.id

    def do_i_still_have_lock(self):
        """
        returns True if:
            - the lock is held
            - the lock's current id is equal to this handle's
        """
        result = self.redis_conn.get(self.key)
        if not result:
            return False
        return self._check_if_same_id(result)

    def check_if_owned(self):
        """
        raises LockExpired if this handle no longer owns its key.
        """
        if not self.do_i_still_have_lock():
            raise LockExpired(self.key, self.id)

    @classmethod
    def get_existing(cls, key, expected_id, redis_conn):
        """
        instantiates a handle for a given lock, which should
        already have been acquired.

        if the key is unlocked, or if its ID does not match
        `expected_id`, raises `LockExpired`
        """
        data = redis_conn.get(key)
        print(data)
        if not data:
            raise LockExpired(key, expected_id)
        instance = cls.deserialize(redis_conn=redis_conn, data=data)
        if instance.id != expected_id:
            raise LockExpired(key, expected_id)
        return instance

    def release(self, ignore_failure=False):
        """
        Releases the held lock, *if* the lock's current
        ID is equal to this handle's.

        On failure, Raises `LockNotOwned` if `ignore_failure` is not True.
        """
        with self.redis_conn.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(self.key)
                    current_value = pipe.get(self.key)
                    if not current_value or not self._check_if_same_id(current_value):
                        if ignore_failure:
                            return False
                        raise LockNotOwned(self.key, self.id)
                    pipe.multi()
                    pipe.delete(self.key)
                    pipe.execute()
                    break
                except WatchError:
                    continue
                finally:
                    pipe.reset()

    @contextlib.contextmanager
    def releasing(self, ignore_failure=False):
        """
        Convenience context manager: makes sure lock is released at end of scope.
        """
        try:
            yield
        finally:
            try:
                self.release()
            except LockNotOwned:
                if not ignore_failure:
                    raise
