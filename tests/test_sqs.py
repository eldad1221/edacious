import os
import unittest
from dotenv import load_dotenv
from edacious.sqs import EventListener

load_dotenv()
SQS_URL = os.getenv('SQS_URL')


class SqsTestCase(unittest.TestCase):

    def test_receive_evens(self):
        listener = EventListener(sqs_url=SQS_URL)
        events = listener.fetch()
        self.assertIsNotNone(events)

    def test_receive_and_delete_evens(self):
        listener = EventListener(sqs_url=SQS_URL)
        events = listener.fetch()
        self.assertIsNotNone(events)
        for event in events:
            listener.event_handling_done(event=event)
            print(f'Deleted event: {event}')


if __name__ == '__main__':
    unittest.main()
