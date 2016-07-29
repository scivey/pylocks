import unittest
from pylocks.core.lock_handle_data import LockHandleData
from pylocks.core.lock_request import LockRequest

class TestLockHandleData(unittest.TestCase):
    def test_serialize(self):
        handle = LockHandleData(
            request=LockRequest(key='k', request_time=70, initial_ttl=1000, lock_arity=2, lock_prefix='yes'),
            id='id',
            acquired_at='atime'
        )
        handle2 = LockHandleData.deserialize(handle.serialize())
        self.assertEqual('k', handle2.key)
