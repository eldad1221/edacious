import unittest
from edacious import event_handler, event_processing, EVENT_TYPE_HANDLERS


@event_handler(event_type='hello-world')
@event_handler(event_type='hello-world-2')
def test_function(event: dict):
    print(f'Test function say {event}')


@event_handler(event_type='hello-world')
def test_function_2(event: dict):
    print(f'Test function 2 say {event}')


@event_handler(event_type='hello-world')
@event_handler(event_type='hello-world-3')
def test_function_3(event: dict):
    print(f'Test function 3 say {event}')


class DecoratorTestCase(unittest.TestCase):

    def test_decorator(self):
        print(EVENT_TYPE_HANDLERS)
        event_processing(event_type='hello-world-3', event={'text': 'Hello'})
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
