import unittest
from pylocks.core.lock_lease_data import LockLeaseData
from pylocks.core.lock_request import LockRequest

class TestLockLeaseData(unittest.TestCase):
    def test_serialize(self):
        handle = LockLeaseData(
            request=LockRequest(key='k', request_time=70, initial_ttl=1000, lock_arity=2, lock_prefix='yes'),
            id='id',
            acquired_at='atime'
        )
        handle2 = LockLeaseData.deserialize(handle.serialize())
        self.assertEqual('k', handle2.key)
