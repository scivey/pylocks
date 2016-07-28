import unittest
import json
import time
from pylocks.errors import LockAlreadyHeld, LockExpired, LockNotOwned, InvalidLockValue
from pylocks.util import make_id
from pylocks import serialization
from .key_formatter import KeyFormatter
from .lock_request import LockRequest

class LockHandleData(serialization.Serializable):
    def __init__(self, request, id, acquired_at):
        self.request = request
        self.id = id
        self.acquired_at = acquired_at

    @property
    def key(self):
        return self.request.key

    def __eq__(self, other):
        return self.key == other.key and self.id == other.id

    def __hash__(self):
        return hash((self.key, self.id))

    def __repr__(self):
        return '<LockHandleData key=%r id=%r />' % (self.key, self.id)


class TestLockHandleData(unittest.TestCase):
    def test_serialize(self):
        handle = LockHandleData(
            request=LockRequest(key='k', request_time=70, initial_ttl=1000, lock_arity=2, lock_prefix='yes'),
            id='id',
            acquired_at='atime'
        )
        handle2 = LockHandleData.deserialize(handle.serialize())
        self.assertEqual('k', handle2.key)
