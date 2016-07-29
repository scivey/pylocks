import unittest
from pylocks.serialization import Serializable
from pylocks.errors import SerializationError

class Something(Serializable):
    def __init__(self, x, y):
        self.x = x
        self.y = y

class Something2(Serializable):
    def __init__(self, x, y):
        self.x = x
        self.y = y

class TestSerialization(unittest.TestCase):
    def test_serializable_sanity(self):

        something = Something(x=10, y=205)
        something_again = Something.deserialize(something.serialize())
        self.assertEqual(10, something_again.x)
        self.assertEqual(205, something_again.y)

    def test_serializable_type_check(self):
        something = Something(x=10, y=205)
        something2 = Something2(x=10, y=205)
        self.assertTrue(isinstance(
            Something.deserialize(something.serialize()),
            Something
        ))
        self.assertTrue(isinstance(
            Something2.deserialize(something2.serialize()),
            Something2
        ))
        with self.assertRaises(SerializationError):
            Something2.deserialize(something.serialize())
        with self.assertRaises(SerializationError):
            Something.deserialize(something2.serialize())
