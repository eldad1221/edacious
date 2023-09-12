import uuid
from main import get_redis
import unittest


class RedisTestCase(unittest.TestCase):

    def test_something(self):
        k = 'test123'
        v = str(uuid.uuid4())
        get_redis().set(name=k, value=v)
        self.assertEqual(v, get_redis().get(name=k))


if __name__ == '__main__':
    unittest.main()
