import unittest
from edacious.sqs import receive_events, delete_event


class SqsTestCase(unittest.TestCase):

    def test_receive_evens(self):
        events = receive_events()
        self.assertIsNotNone(events)

    def test_receive_and_delete_evens(self):
        events = receive_events()
        self.assertIsNotNone(events)
        for event in events:
            delete_event(receipt_handle=event['ReceiptHandle'])
            print(f'Deleted event: {event}')


if __name__ == '__main__':
    unittest.main()
