import unittest

class RedisTest(unittest.TestCase):
    def setUp(self):
        import redislite
        self.r = redislite.StrictRedis()

